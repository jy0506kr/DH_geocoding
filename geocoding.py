import pandas as pd
import numpy as np
import geopandas as gpd
import streamlit as st
import requests, re, os, io, zipfile, tempfile
from pyproj import Transformer
from shapely.geometry import Point
from io import BytesIO
import concurrent.futures

# --- 1. 기능 함수 정의 ---

# 주소 -> 경위도 (V-World API)
def geocoding_latlong(address, api_key):
    base_url = "https://api.vworld.kr/req/address?"

    # [핵심 수정 1] 브라우저인 척 속이는 '헤더' 추가
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://share.streamlit.io/" 
    }

    def get_parms(query, type_hint='PARCEL'):
        return {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4326",
            "address": query,
            "refine": "true",
            "simple": "false",
            "format": "json",
            "type": type_hint,
            "key": api_key}

    try:
        # headers=headers 추가
        response = requests.get(base_url, params=get_parms(address, 'PARCEL'), headers=headers, timeout=10)
        
        # [핵심 수정 2] 응답이 200(성공)이 아니면 에러 내용을 반환
        if response.status_code != 200:
            return {'found': False, 'error': f"HTTP 에러: {response.status_code}"}

        json_data = response.json()
        
        # API 결과 상태가 OK가 아니면 메시지 확인
        if json_data['response']['status'] != 'OK':
            # 결과가 없는 경우 바로 도로명 시도
            pass 
        else:
            res = json_data['response']['result']['point']
            return {'lat': float(res['y']), 'lng': float(res['x']), 'found': True, 'level': 'exact'}

        # 2차 시도: 도로명 주소
        response = requests.get(base_url, params=get_parms(address, 'ROAD'), headers=headers, timeout=10)
        json_data = response.json()
        if json_data['response']['status'] == 'OK':
            res = json_data['response']['result']['point']
            return {'lat': float(res['y']), 'lng': float(res['x']), 'found': True, 'level': 'exact'}
            
    except Exception as e:
        # [핵심 수정 3] 에러를 숨기지 말고 텍스트로 반환 (무슨 에러인지 보기 위함)
        return {'found': False, 'error': f"시스템 에러: {str(e)}"}

    # ... (정제 로직 생략, 필요하다면 위와 동일하게 headers 추가) ...

    return {'found': False, 'error': '결과 없음 (V-World 응답 확인 필요)'}

# 단일 행 처리를 위한 래퍼 함수 (병렬 처리를 위해 필요)
def process_row(row, addr_col, api_key):
    addr = row[addr_col]
    if pd.isna(addr) or str(addr).strip() == "":
        res = {'found': False, 'error': '빈 값'}
    else:
        res = geocoding_latlong(str(addr), api_key)
    
    # 원본 데이터에 결과 합치기
    row_dict = row.to_dict()
    row_dict.update(res)
    return row_dict

# 경위도 -> TM 변환
def convert_tm(lat, lng):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5186") 
    TMY, TMX = transformer.transform(lat, lng)
    return TMX, TMY

# SHP 파일 생성 및 압축
def convert_to_shp_zip(df, file_name_prefix):
    valid_df = df[df['found'] == True].copy()
    
    if valid_df.empty:
        return None

    geometry = [Point(xy) for xy in zip(valid_df['lng'], valid_df['lat'])]
    gdf_out = gpd.GeoDataFrame(valid_df, geometry=geometry, crs="EPSG:4326")
    
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

# --- 2. Streamlit UI ---

st.set_page_config(page_title="지오코딩 및 SHP 변환기", page_icon="🗺️")
st.title("🗺️ 지오코딩 및 SHP 변환기")

# API 키 보안을 위해 기본값 제거 (사용자가 입력하도록 유도)
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
                
                # [중요 수정] 병렬 처리 (ThreadPoolExecutor)
                # max_workers=10 : 동시에 10개씩 요청 (너무 높으면 API 차단될 수 있음)
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    # 각 행에 대해 process_row 함수를 예약
                    future_to_row = {executor.submit(process_row, row, addr_col, api_key_input): i for i, row in df.iterrows()}
                    
                    # 작업이 끝나는 순서대로 결과 수집
                    for future in concurrent.futures.as_completed(future_to_row):
                        row_result = future.result()
                        
                        # 좌표 변환 성공 시 TM 좌표 계산
                        if row_result.get('found'):
                            tm_x, tm_y = convert_tm(row_result['lat'], row_result['lng'])
                            row_result['TMX'] = tm_x
                            row_result['TMY'] = tm_y
                            
                        results.append(row_result)
                        
                        # 진행률 업데이트
                        completed_count += 1
                        progress = completed_count / total
                        progress_bar.progress(progress)
                        status_text.text(f"처리 중... {completed_count}/{total}")
                
                # 결과 정렬 (병렬 처리는 순서가 뒤섞일 수 있으므로 인덱스 기준 정렬 필요할 수 있음. 여기선 단순 append)
                result_df = pd.DataFrame(results)
                
                status_text.text("변환 완료!")
                st.write("### 변환 결과")
                st.dataframe(result_df.head())
                
                col1, col2 = st.columns(2)

                # 1. 엑셀 다운로드
                with col1:
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        result_df.to_excel(writer, index=False)
                    processed_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 엑셀 파일 다운로드",
                        data=processed_data,
                        file_name=f"{file_stem}_변환.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                # 2. SHP 다운로드
                with col2:
                    shp_filename_prefix = f"{file_stem}_g60"
                    shp_zip_data = convert_to_shp_zip(result_df, shp_filename_prefix)
                    
                    if shp_zip_data:
                        st.download_button(
                            label="💾 SHP 파일 다운로드 (Zip)",
                            data=shp_zip_data,
                            file_name=f"{shp_filename_prefix}.zip",
                            mime="application/zip"
                        )
                    else:
                        st.warning("변환된 좌표가 없어 SHP 파일을 생성할 수 없습니다.")
            
    except Exception as e:
        st.error(f"오류가 발생했습니다: {e}")