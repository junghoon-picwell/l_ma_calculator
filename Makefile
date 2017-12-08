.ONESHELL:

VERSION_FILE := VERSION
HERE := $(shell pwd)
VSN := $(shell cat ${VERSION_FILE})
OUTPUT := $(HERE)/build/l_ma_calculator-$(VSN).zip
LIB_DIR := $(HERE)/lib

# http://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
# And:
# http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
ver :
	ECHO $(VSN)
	ECHO $(OUTPUT)
	ECHO $(VIRTUAL_ENV)

package : clean
	pip install -r requirements.txt -t $(LIB_DIR)
	zip $(OUTPUT) *.py $(LIB_DIR)/*

clean :
	(rm -rf $(HERE)/build/* || true) || true

