TOP_DIR := .
SRC_DIR := $(TOP_DIR)/src
DIST_DIR := $(TOP_DIR)/dist
TEST_DIR := $(TOP_DIR)/testing
LIB_NAME := skoop
LIB_VERSION := $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)
LIB := $(LIB_NAME)-$(LIB_VERSION)-py3-none-any.whl
TARGET := $(DIST_DIR)/$(LIB)

ifeq ($(OS),Windows_NT)
    PYTHON := py.exe
else
    PYTHON := python3
endif

POETRY := poetry
POETRY_INSTALL := $(POETRY) install 
POETRY_EXPORT := $(POETRY) export
POETRY_BUILD := $(POETRY) build
PIP_INSTALL := $(PYTHON) -m pip install 
PIP_UNINSTALL := $(PYTHON) -m pip uninstall -y
PYTEST := $(POETRY) run pytest -s
BLACK := $(POETRY) run black
RUFF := $(POETRY) run ruff check --fix --ignore E501
ISORT := $(POETRY) run isort
FIND := $(shell which find)
RM := rm -rf
CD := cd

.PHONY: all clean distclean dist test format depends help 

all: dist

install: depends 
	$(POETRY_INSTALL) 

dist: install
	$(POETRY_BUILD)

depends: dependency-check
	$(POETRY_INSTALL) 
	$(POETRY_EXPORT) --without-hashes --format=requirements.txt > requirements.txt

format: dependency-check
	$(RUFF) $(SRC_DIR) $(TEST_DIR)
	$(ISORT) $(SRC_DIR) $(TEST_DIR)
	$(BLACK) $(SRC_DIR) $(TEST_DIR)

clean: 
	$(FIND) $(SRC_DIR) -name \*.pyc -exec rm -f {} \;
	$(FIND) $(SRC_DIR) -name \*.pyo -exec rm -f {} \;
	$(FIND) $(TEST_DIR) -name \*.pyc -exec rm -f {} \;
	$(FIND) $(TEST_DIR) -name \*.pyo -exec rm -f {} \;

distclean: clean
	$(RM) $(DIST_DIR)
	$(RM) $(SRC_DIR)/*.egg-info 
	$(RM) $(TOP_DIR)/.mypy_cache
	$(FIND) $(SRC_DIR) $(TEST_DIR) \( -name __pycache__ -a -type d \) -prune -exec rm -rf {} \;

test: 
	$(PYTEST) $(TEST_DIR)

help:
	$(info TOP_DIR: $(TOP_DIR))
	$(info SRC_DIR: $(SRC_DIR))
	$(info DIST_DIR: $(DIST_DIR))
	$(info TEST_DIR: $(TEST_DIR))
	$(info )
	$(info $$> make [all|dist|install|uninstall|clean|distclean|format|depends|test])
	$(info )
	$(info       all          - build library: [$(LIB)]. This is the default)
	$(info       dist         - build library: [$(LIB)])
	$(info       install      - installs: [$(LIB)])
	$(info       uninstall    - uninstalls: [$(LIB)])
	$(info       clean        - removes build artifacts)
	$(info       distclean    - removes library)
	$(info       format       - format source code)
	$(info       depends      - installs library dependencies)
	$(info       test         - run unit tests)
	$(info       run          - run docker image)
	@true

POETRY_CMD := poetry > /dev/null 2>&1
HAS_POETRY := $(shell $(POETRY_CMD) > /dev/null && echo true || echo false)
PIP_CMD := pip > /dev/null 2>&1
HAS_PIP := $(shell $(PIP_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_POETRY),true)
ifeq ($(HAS_PIP),true)
	SYSTEM_OK = true
endif
endif

dependency-check: check-pip check-poetry

check-pip:
ifneq ($(HAS_PIP),true)
	$(PYTHON) -m pip install --upgrade pip 
endif
	
check-poetry: 
ifneq ($(HAS_POETRY),true)
	$(PIP_INSTALL) poetry
endif

