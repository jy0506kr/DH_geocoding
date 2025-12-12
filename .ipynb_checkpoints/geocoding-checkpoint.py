import pandas as pd
import numpy as np
import geopandas as gpd
import streamlit as st
import requests, re, os, io, zipfile, tempfile
import concurrent.futures
from pyproj import Transformer
from shapely.geometry import Point
from io import BytesIO

# --- 1. 기능 함수 정의 ---

def geocoding_latlong(address, api_key):
    # HTTPS 사용
    base_url = "https://api.vworld.kr/req/address?"

    # [중요] 봇 탐지 회피용 헤더 & API 키 활용처 검증용 Referer
    # 만약 V-World 키 설정에 URL 제한을 걸었다면, 이 Referer가 일치해야 합니다.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://share.streamlit.io/" 
    }

    def get_parms(query, type_hint='PARCEL'):
        return {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4019",
            "address": query,
            "refine": "true",
            "simple": "false",
            "format": "json",
            "type": type_hint,
            "key": api_key
        }

    try:
        # 1차 시도: 지번 주소 (PARCEL)
        response = requests.get(base_url, params=get_parms(address, 'PARCEL'), headers=headers, timeout=10)
        
        if response.status_code != 200:
            return {'found': False, 'error': f"HTTP 접속 오류 ({response.status_code})"}

        data = response.json()
        status = data['response']['status']

        # [핵심 수정] 성공(OK)이 아니면, V-World가 준 에러 코드를 반환
        if status == 'OK':
            res = data['response']['result']['point']
            return {'lat': float(res['y']), 'lng': float(res['x']), 'found': True, 'level': 'exact'}
        
        # OK가 아닌데, 결과가 없어서(NOT_FOUND) 그런 거라면 도로명 주소로 재시도
        if status == 'NOT_FOUND':
            pass # 아래 도로명 검색으로 넘어감
        else:
            # INVALID_KEY, INCORRECT_KEY 등 심각한 에러는 바로 반환
            msg = data['response'].get('text', '') # 에러 메시지가 있다면 가져옴
            return {'found': False, 'error': f"API 에러: {status} ({msg})"}

        # 2차 시도: 도로명 주소 (ROAD)
        response = requests.get(base_url, params=get_parms(address, 'ROAD'), headers=headers, timeout=10)
        data = response.json()
        status = data['response']['status']

        if status == 'OK':
            res = data['response']['result']['point']
            return {'lat': float(res['y']), 'lng': float(res['x']), 'found': True, 'level': 'exact'}
        elif status != 'NOT_FOUND':
             # 여기서도 OK도 아니고 NOT_FOUND도 아니면 에러 코드 반환
             msg = data['response'].get('text', '')
             return {'found': False, 'error': f"API 에러: {status} ({msg})"}

    except Exception as e:
        return {'found': False, 'error': f"시스템 에러: {str(e)}"}

    return {'found': False, 'error': '주소 불명 (결과 없음)'}

# (나머지 함수들은 그대로 유지)
def process_row(row, addr_col, api_key):
    addr = row[addr_col]
    if pd.isna(addr) or str(addr).strip() == "":
        res = {'found': False, 'error': '빈 값'}
    else:
        res = geocoding_latlong(str(addr), api_key)
    row_dict = row.to_dict()
    row_dict.update(res)
    return row_dict

def convert_tm(lat, lng):
    transformer = Transformer.from_crs("EPSG:4019", "EPSG:5186") 
    TMY, TMX = transformer.transform(lat, lng)
    return TMX, TMY

def convert_to_shp_zip(df, file_name_prefix):
    valid_df = df[df['found'] == True].copy()
    if valid_df.empty:
        return None
    geometry = [Point(xy) for xy in zip(valid_df['lng'], valid_df['lat'])]
    gdf_out = gpd.GeoDataFrame(valid_df, geometry=geometry, crs="EPSG:4019")
    for col in gdf_out.columns:
        if gdf_out[col].dtype == 'object':
            gdf_out[col] = gdf_out[col].astype(str)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_filepath = os.path.join(temp_dir, f"{file_name_prefix}.shp")
        gdf_out.to_file(temp_filepath, driver='ESRI Shapefile', encoding='cp949')
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for filename in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, filename)
                zip_file.write(file_path, filename)
        return zip_buffer.getvalue()

# --- Streamlit UI ---
st.set_page_config(page_title="지오코딩 및 SHP 변환기", page_icon="🗺️")
st.title("🗺️ 지오코딩 및 SHP 변환기")
api_key_input = st.text_input("V-World API Key", type="password", placeholder="API 키를 입력하세요")
uploaded_file = st.file_uploader("엑셀 또는 CSV 파일을 업로드하세요", type=['xlsx', 'xls', 'csv'])

if uploaded_file:
    try:
        file_stem = os.path.splitext(uploaded_file.name)[0]
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file, encoding='cp949')
        else:
            df = pd.read_excel(uploaded_file)
        
        st.write("### 데이터 미리보기")
        st.dataframe(df.head())
        addr_col = st.selectbox("주소가 들어있는 열을 선택하세요", df.columns)
        
        if st.button("좌표 변환 시작"):
            if not api_key_input:
                st.error("API 키를 입력해주세요!")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                results = []
                total = len(df)
                completed_count = 0
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_row = {executor.submit(process_row, row, addr_col, api_key_input): i for i, row in df.iterrows()}
                    for future in concurrent.futures.as_completed(future_to_row):
                        row_result = future.result()
                        if row_result.get('found'):
                            tm_x, tm_y = convert_tm(row_result['lat'], row_result['lng'])
                            row_result['TMX'] = tm_x
                            row_result['TMY'] = tm_y
                        results.append(row_result)
                        completed_count += 1
                        progress_bar.progress(completed_count / total)
                        status_text.text(f"처리 중... {completed_count}/{total}")
                
                result_df = pd.DataFrame(results)
                status_text.text("변환 완료!")
                st.write("### 변환 결과 (에러 메시지 확인)")
                # 에러 컬럼이 잘 보이게 앞쪽 데이터 표시
                st.dataframe(result_df[['error'] if 'error' in result_df.columns else result_df.columns].head())
                
                col1, col2 = st.columns(2)
                with col1:
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        result_df.to_excel(writer, index=False)
                    st.download_button("📥 엑셀 파일 다운로드", output.getvalue(), f"{file_stem}_변환.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with col2:
                    shp_data = convert_to_shp_zip(result_df, f"{file_stem}_g60")
                    if shp_data:
                        st.download_button("💾 SHP 파일 다운로드", shp_data, f"{file_stem}_g60.zip", "application/zip")
                    else:
                        st.warning("변환 성공한 데이터가 없어 SHP를 만들 수 없습니다.")
    except Exception as e:
        st.error(f"오류 발생: {e}")