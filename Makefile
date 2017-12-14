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
	cp $(HERE)/lambda.cfg $(SRC_DIR)
	$(if $(shell python add_benefit_file.py), \
		python benefit_to_file.py s3://picwell.sandbox.medicare/ma_benefits/cms_2018_pbps_20171005.json $(SRC_DIR)/benefits.py)

	pip install -r lambda_package_requirements.txt -t $(LIB_DIR)
	#pip install ../misscleo -t $(LIB_DIR)

	# Using subshell to change directory
	(cd $(SRC_DIR); zip -r $(OUTPUT) . -i \*.{py,cfg} -x "test*"; cd $(HERE))

clean :
	[ -d $(LIB_DIR) ] && rm -rf $(LIB_DIR) && (rm -rf $(HERE)/build/* || true) || true
	[ -f $(SRC_DIR)/lambda.cfg ] && rm $(SRC_DIR)/lambda.cfg || true
	[ -f $(SRC_DIR)/benefits.py ] && rm $(SRC_DIR)/benefits.py || true

