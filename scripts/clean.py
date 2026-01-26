import pandas as pd
import re
from pathlib import Path

def main():
  
    BASE_DIR = Path("/opt/airflow")

    INPUT_PATH = BASE_DIR / "data" / "raw" / "Software Engineer Salaries.csv"
    OUTPUT_PATH = BASE_DIR / "data" / "processed" / "jobs_cleaned.csv"

    
    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded raw data: {len(df)} rows")

    df = df.drop_duplicates()
    print(f"After deduplication: {len(df)} rows")

    
    df["Company"] = df["Company"].astype(str)
    df["Company"] = df["Company"].str.replace("nan", "data not available", regex=False)
    df["Company"] = df["Company"].str.lower().str.strip()

    df["Company Score"] = pd.to_numeric(df["Company Score"], errors="coerce")
    df["Job Title"] = df["Job Title"].str.lower().str.strip()

    
    def extract_job(title: str) -> str:
        title = title.lower()

        if "software development engineer in test" in title or "sdet" in title:
            return "qa/test engineer"
        if "qa" in title or "test engineer" in title:
            return "qa/test engineer"
        if "data engineer" in title:
            return "data engineer"
        if "machine learning engineer" in title:
            return "machine learning engineer"
        if "ai engineer" in title or "artificial intelligence" in title:
            return "ai engineer"
        if "devops engineer" in title:
            return "devops engineer"
        if "cloud engineer" in title:
            return "cloud engineer"
        if (
            ("software" in title or "backend" in title or "frontend" in title)
            and ("engineer" in title or "developer" in title)
        ):
            return "software engineer"
        return "other"

    df["job_family"] = df["Job Title"].apply(extract_job)

   
    def extract_seniority(title: str) -> str:
        title = title.lower()

        if "entry level to senior level" in title:
            return "entry to senior"

        if any(w in title for w in ["lead", "manager", "director", "head of", "principal"]):
            return "leadership"

        if re.search(r"\b(senior|sr\.?|iii|iv|v|3|4)\b", title):
            return "senior"

        if re.search(r"\b(junior|jr\.?|entry|i|ii|1|2)\b", title):
            return "junior"

        if "mid" in title:
            return "mid-level"

        return "not specified"

    df["seniority"] = df["Job Title"].apply(extract_seniority)

    
    def extract_specialization(title: str):
        title = title.lower()
        specs = set()

        if re.search(r"\bback\s?end\b", title):
            specs.add("backend")
        if re.search(r"\bfront\s?end\b", title):
            specs.add("frontend")
        if "full stack" in title:
            specs.add("fullstack")
        if "devops" in title:
            specs.add("devops")
        if "cloud" in title:
            specs.add("cloud")
        if "python" in title:
            specs.add("python")
        if "java" in title:
            specs.add("java")
        if "javascript" in title:
            specs.add("javascript")
        if "c#" in title:
            specs.add("c#")
        if "c++" in title:
            specs.add("c++")

        return ", ".join(sorted(specs)) if specs else None

    df["specialization"] = df["Job Title"].apply(extract_specialization)

    
    df["Location"] = df["Location"].astype(str).str.strip()
    df[["city", "state"]] = df["Location"].str.split(",", n=1, expand=True)

    df["is_remote"] = df["Location"].str.contains("Remote", case=False)
    df.loc[df["is_remote"], ["city", "state"]] = [None, None]

    df["city"] = df["city"].str.strip()
    df["state"] = df["state"].str.strip()
    df["country"] = "United States"

    
    df["date_since_posted"] = df["Date"].str.extract(r"([0-9]+)")

    df["Salary"] = (
        df["Salary"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
    )

    def parse_salary(text):
        numbers = re.findall(r"\d+", str(text))
        if len(numbers) == 2:
            return int(numbers[0]) * 1000, int(numbers[1]) * 1000
        return None, None

    df[["salary_min", "salary_max"]] = df["Salary"].apply(
        lambda x: pd.Series(parse_salary(x))
    )

    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved cleaned data to {OUTPUT_PATH}")



if __name__ == "__main__":
    main()
