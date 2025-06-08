.PHONY: run dev clean start

run:
	PYTHONPATH=. .venv/bin/python src/main.py

dev:
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

clean:
	rm -rf __pycache__ .venv *.db

start: clean dev run