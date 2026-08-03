.PHONY: format lint package test

format:
	.venv/bin/ruff format .

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

test:
	.venv/bin/pytest

package:
	mkdir -p dist
	rm -f dist/test_task_bazis.zip
	zip -qr dist/test_task_bazis.zip . \
		-x '.git/*' \
		-x '.idea/*' \
		-x '.venv/*' \
		-x '.env' \
		-x '.coverage' \
		-x '.pytest_cache/*' \
		-x '.ruff_cache/*' \
		-x '*__pycache__/*' \
		-x '*.py[cod]' \
		-x '.DS_Store' \
		-x '*/.DS_Store' \
		-x 'dist/*' \
		-x 'htmlcov/*' \
		-x 'tmp/*'
	@echo "Created dist/test_task_bazis.zip"
