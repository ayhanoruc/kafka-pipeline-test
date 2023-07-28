# this is a confluent kafka data pipeline template


This repo helps us to know how to publish and consume data to and from kafka confluent in json format.

Step1: Create a conda environment and be sure that the VS path exists in environment variables.

```
conda --version
conda create -p <env_name> python=3.11 -y
conda activate ./<env_name>
```

Step2: Install requirements.txt
```
pip install -r requirements.txt
```

Step 3: Create a github repo, then do the following
```
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/<username>/<project_name>.git
git push -u origin main
```