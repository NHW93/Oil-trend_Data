import pandas as pd
import requests

# 국제 유가 API 호출 (예제: 두바이유, 브렌트유, WTI)
API_URL = "https://api.example.com/oil-prices"  # 실제 API URL 입력

response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    
    # 데이터프레임 변환
    df = pd.DataFrame(data)
    
    # CSV 파일로 저장
    df.to_csv("data.csv", index=False, encoding="utf-8")

    print("CSV 파일이 성공적으로 업데이트되었습니다.")
else:
    print("API 호출 실패:", response.status_code)
