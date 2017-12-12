.ONESHELL:

VERSION_FILE := VERSION
HERE := $(shell pwd)
VSN := $(shell cat ${VERSION_FILE})
OUTPUT := $(HERE)/build/l_ma_calculator-$(VSN).zip
SRC_DIR := $(HERE)/lambda_package
LIB_DIR := $(SRC_DIR)/lib

# http://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
# And:
# http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
ver :
	ECHO $(VSN)
	ECHO $(OUTPUT)
	ECHO $(VIRTUAL_ENV)

package : clean
	pip install -r lambda_package_requirements.txt -t $(LIB_DIR)
	# Using subshell to change directory
	(cd $(SRC_DIR); zip -r $(OUTPUT) . -i \*.{py,cfg} -x "test*"; cd $(HERE))

clean :
	[ -d $(LIB_DIR) ] && rm -rf $(LIB_DIR) && (rm -rf $(HERE)/build/* || true) || true

