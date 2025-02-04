import requests
import pandas as pd
import json
from datetime import datetime
import os
from dotenv import load_dotenv
from github import Github

# ✅ 환경 변수 로드
load_dotenv()

# ✅ GitHub 설정
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
GITHUB_REPO_NAME = "NHW93/Oil-trend_Data"
GITHUB_CSV_PATH = "oil_data.csv"

# ✅ API 키 설정 (GitHub Secrets 사용)
OPINET_API_KEY = os.getenv("OPINET_API_KEY")

# ✅ 국내 유가 API (최근 7일치 데이터)
OIL_API_URL = f"https://www.opinet.co.kr/api/avgRecentPrice.do?out=json&code={OPINET_API_KEY}"

# ✅ CSV 파일 경로 (로컬 테스트용)
CSV_FILE = "oil_data.csv"

# ✅ 국내 유가 코드 매핑 (CSV 컬럼명과 API 응답 값 매칭)
PRODUCT_MAP = {
    "B034": "Premium Gasoline",
    "B027": "Regular Gasoline",
    "D047": "Diesel"
}

def fetch_recent_oil_prices():
    """✅ 최근 7일치 국내 유가 데이터 가져오기"""
    response = requests.get(OIL_API_URL)
    if response.status_code == 200:
        data = json.loads(response.text)
        if "RESULT" in data and "OIL" in data["RESULT"]:
            oil_data = []
            for item in data["RESULT"]["OIL"]:
                date = item["DATE"]  # API에서 제공하는 날짜
                oil_entry = {"Date": date}
                for prod_code, col_name in PRODUCT_MAP.items():
                    oil_entry[col_name] = float(item.get(prod_code, 0))
                oil_data.append(oil_entry)
            return oil_data
    print("❌ 국내 유가 API 호출 실패:", response.status_code)
    return None

def update_csv():
    """✅ 기존 CSV 파일을 업데이트"""
    # ✅ API에서 최근 7일치 데이터 가져오기
    recent_oil_prices = fetch_recent_oil_prices()
    if recent_oil_prices is None:
        print("❌ 데이터 가져오기 실패, 업데이트 중단")
        return False

    # ✅ CSV 파일이 존재하는 경우 불러오기
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
    else:
        df = pd.DataFrame(columns=["Date", "Premium Gasoline", "Regular Gasoline", "Diesel"])

    # ✅ 새로운 데이터프레임 생성
    df_new = pd.DataFrame(recent_oil_prices)

    # ✅ 기존 데이터에 추가 (중복 제거)
    df = pd.concat([df, df_new]).drop_duplicates(subset=["Date"]).sort_values(by="Date")

    # ✅ CSV 파일로 저장
    df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    print("✅ 국내 유가 데이터 업데이트 완료")
    return True

def push_to_github():
    """✅ 업데이트된 CSV 파일을 GitHub에 푸시"""
    g = Github(ACCESS_TOKEN)
    repo = g.get_repo(GITHUB_REPO_NAME)

    try:
        # ✅ 기존 파일 가져오기 (없으면 예외 발생)
        contents = repo.get_contents(GITHUB_CSV_PATH)

        # ✅ 파일 내용 읽기
        with open(GITHUB_CSV_PATH, "r", encoding="utf-8-sig") as file:
            content = file.read()

        # ✅ GitHub에 파일 업데이트
        repo.update_file(contents.path, f"자동 업데이트 - {datetime.today().strftime('%Y-%m-%d')}", content, contents.sha)
        print("✅ GitHub에 CSV 파일 업데이트 완료")

    except Exception as e:
        print(f"❌ 오류: `{GITHUB_CSV_PATH}` 파일을 찾을 수 없습니다! GitHub 저장소에 존재하는지 확인하세요.")
        print(f"🔍 상세 오류 메시지: {e}")
        exit(1)  # 🚨 오류 발생 시 프로그램 종료

def main():
    """✅ 전체 실행 함수"""
    print("🚀 국내 유가 데이터 업데이트 시작")
    if update_csv():
        push_to_github()
        print("✅ 모든 업데이트 완료")
    else:
        print("❌ 업데이트 실패")


if __name__ == "__main__":
    main()
