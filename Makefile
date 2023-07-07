
# Makefile

test:
	pytest -v -l --full-trace --color=yes --code-highlight=yes

update:
	pip install -r requirements.txt
	pip install --upgrade pip
	