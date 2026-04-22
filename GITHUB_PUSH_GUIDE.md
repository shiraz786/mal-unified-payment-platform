# GitHub Push Guide for Shiraz

## 1) Create a new repository in GitHub
- Log in to GitHub
- Click **New repository**
- Repository name: `mal-unified-payment-platform`
- Keep it **Public**
- Do **not** add README there, because we already have one
- Click **Create repository**

## 2) Open terminal in VS Code inside the project folder
Use the folder:
```bash
mal_payment_platform
```

## 3) Run these commands one by one
```bash
git init
git branch -M main
git add .
git commit -m "Initial submission for Mal data engineering assessment"
git remote add origin https://github.com/shiraz786/mal-unified-payment-platform.git
git push -u origin main
```

## 4) If Git asks for identity, run this first
```bash
git config --global user.name "Shiraz Hussain"
git config --global user.email "YOUR_GITHUB_EMAIL"
```
Then repeat:
```bash
git add .
git commit -m "Initial submission for Mal data engineering assessment"
git push -u origin main
```

## 5) If remote already exists
```bash
git remote remove origin
git remote add origin https://github.com/shiraz786/mal-unified-payment-platform.git
git push -u origin main
```

## 6) Final link to submit
Use this format:
```text
https://github.com/shiraz786/mal-unified-payment-platform
```

## 7) Very important before submitting
Check that GitHub shows these files:
- README.md
- requirements.txt
- src/pipeline.py
- src/contracts.py
- src/sql_queries.sql
- data/input/*.csv
- output/*
- Migration_Architecture_Strategy.pdf

## 8) Nice extra touch
After push, add this line at the top of README:
> Assessment submission for Mal - Senior Data Engineer (Cross-Product Data Platform Design)
