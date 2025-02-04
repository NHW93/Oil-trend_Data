import requests
import pandas as pd
import time
import json
from datetime import datetime
from github import Github
import os
from dotenv import load_dotenv

# ✅ 환경 변수 로드
load_dotenv()

# ✅ API 키 설정 (GitHub Secrets 사용)
OPINET_API_KEY = os.getenv("OPINET_API_KEY")
EXCHANGE_RATE_API_KEY = os.getenv("EXIMBANK_API_KEY")
GITHUB_TOKEN = os.getenv("ACCESS_TOKEN")

# ✅ GitHub 저장소 및 파일 경로
GITHUB_REPO_NAME = "NHW93/Oil-trend_Data"
GITHUB_CSV_URL = "https://raw.githubusercontent.com/NHW93/Oil-trend_Data/main/oil_data.csv"
GITHUB_CSV_PATH = "oil_data.csv"

# ✅ API URL 설정
OIL_API_URL = f"https://www.opinet.co.kr/api/avgAllPrice.do?out=json&code={OPINET_API_KEY}"
EXCHANGE_RATE_API_URL = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON"

# ✅ 국내 유가 코드 매핑
PRODUCT_MAP = {
    "B034": "Premium Gasoline",
    "B027": "Regular Gasoline",
    "D047": "Diesel"
}

def load_csv_from_github():
    """GitHub 리포지토리에서 CSV 파일을 다운로드"""
    response = requests.get(GITHUB_CSV_URL)
    if response.status_code == 200:
        with open(GITHUB_CSV_PATH, "wb") as f:
            f.write(response.content)
        print("✅ CSV 파일 다운로드 완료")
        return pd.read_csv(GITHUB_CSV_PATH)
    else:
        print("❌ CSV 파일 다운로드 실패:", response.status_code)
        return None

def fetch_oil_prices():
    """✅ 국내 유가 데이터 가져오기"""
    response = requests.get(OIL_API_URL)
    if response.status_code == 200:
        data = json.loads(response.text)
        oil_data = {}
        if "RESULT" in data and "OIL" in data["RESULT"]:
            for item in data["RESULT"]["OIL"]:
                prod_code = item.get("PRODCD")
                if prod_code in PRODUCT_MAP:
                    oil_data[PRODUCT_MAP[prod_code]] = float(item.get("PRICE", 0))
        return oil_data
    return None

def fetch_exchange_rate():
    """✅ USD 환율 데이터 가져오기 (재시도 포함)"""
    today = datetime.today().strftime("%Y%m%d")
    retries = 3  # 최대 재시도 횟수

    for attempt in range(retries):
        try:
            response = requests.get(
                EXCHANGE_RATE_API_URL,
                params={"authkey": EXIMBANK_API_KEY, "searchdate": today, "data": "AP01"},
                timeout=10  # 10초 타임아웃 설정
            )

            if response.status_code == 200:
                data = response.json()
                for item in data:
                    if item["cur_unit"] == "USD":
                        return float(item["deal_bas_r"].replace(",", ""))
            else:
                print(f"⚠️ 환율 API 응답 실패: {response.status_code}, 재시도 {attempt + 1}/{retries}")
        
        except requests.exceptions.RequestException as e:
            print(f"⚠️ 요청 실패: {e}, 재시도 {attempt + 1}/{retries}")

        time.sleep(5)  # 5초 대기 후 재시도

    print("❌ 환율 API 3회 요청 실패, 업데이트 중단")
    return None

def update_csv(df):
    """✅ 기존 CSV 파일을 업데이트"""
    today = datetime.today().strftime('%Y-%m-%d')

    # ✅ 새로운 데이터 가져오기
    oil_prices = fetch_oil_prices()
    exchange_rate = fetch_exchange_rate()

    if oil_prices is None or exchange_rate is None:
        print("❌ 데이터 가져오기 실패, 업데이트 중단")
        return False

    # ✅ 기존 데이터에 오늘 날짜 데이터가 있는지 확인
    if today in df["Date"].values:
        print(f"🔄 {today} 데이터가 이미 존재하여 업데이트됨")
        df.loc[df["Date"] == today, ["Exchange Rate", "Premium Gasoline", "Regular Gasoline", "Diesel"]] = [
            exchange_rate, oil_prices["Premium Gasoline"], oil_prices["Regular Gasoline"], oil_prices["Diesel"]
        ]
    else:
        print(f"➕ 새로운 데이터 추가 ({today})")
        new_data = pd.DataFrame([{
            "Date": today,
            "WTI": None,  # WTI 데이터 추가 가능
            "Brent": None,  # Brent 데이터 추가 가능
            "Dubai": None,  # Dubai 데이터 추가 가능
            "Exchange Rate": exchange_rate,
            "Premium Gasoline": oil_prices["Premium Gasoline"],
            "Regular Gasoline": oil_prices["Regular Gasoline"],
            "Diesel": oil_prices["Diesel"]
        }])
        df = pd.concat([df, new_data], ignore_index=True)

    # ✅ CSV 파일 저장
    df.to_csv(GITHUB_CSV_PATH, index=False, encoding="utf-8-sig")
    return True

def push_to_github():
    """✅ 업데이트된 CSV 파일을 GitHub에 푸시"""
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(GITHUB_REPO_NAME)

    # ✅ 기존 파일 가져오기
    contents = repo.get_contents(GITHUB_CSV_PATH)
    
    with open(GITHUB_CSV_PATH, "r", encoding="utf-8-sig") as file:
        content = file.read()

    repo.update_file(contents.path, f"자동 업데이트 - {datetime.today().strftime('%Y-%m-%d')}", content, contents.sha)
    print("✅ GitHub에 CSV 파일 업데이트 완료")

def main():
    """✅ 전체 실행 함수"""
    print("🚀 유가 및 환율 데이터 업데이트 시작")

    df = load_csv_from_github()
    if df is not None and update_csv(df):
        push_to_github()
        print("✅ 모든 업데이트 완료")
    else:
        print("❌ 업데이트 실패")

if __name__ == "__main__":
    main()
