.ONESHELL:

VERSION_FILE := VERSION
HERE := $(shell pwd)
VSN := $(shell cat ${VERSION_FILE})
OUTPUT := $(HERE)/build/l_ma_calculator-$(VSN).zip

SRC_DIR := $(HERE)/lambda_client
TAR_DIR := $(HERE)/lambda_package
LIB_DIR := $(TAR_DIR)/lib

BENEFIT := $(shell python get_benefit_file.py)

# http://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
# And:
# http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
ver :
	ECHO $(VSN)
	ECHO $(OUTPUT)
	ECHO $(VIRTUAL_ENV)

package : clean
	cp $(HERE)/VERSION $(TAR_DIR)
	cp $(SRC_DIR)/lambda.cfg $(TAR_DIR)
	cp $(SRC_DIR)/config_info.py $(TAR_DIR)
	cp $(SRC_DIR)/shared_utils.py $(TAR_DIR)

	# Generate hard-coded benefits file if needed:
	$(if $(BENEFIT), python benefit_to_file.py $(BENEFIT) $(TAR_DIR)/benefits.py)

	pip install -r lambda_package_requirements.txt -t $(LIB_DIR)
	# pip install ../misscleo -t $(LIB_DIR)

	# Using subshell to change directory
	(cd $(TAR_DIR); zip -r $(OUTPUT) . -i \*.{py,cfg} -x "test*"; cd $(HERE))

clean :
	[ -d $(LIB_DIR) ] && rm -rf $(LIB_DIR) && (rm -rf $(HERE)/build/* || true) || true

	# Files shared by client and lambda package:
	[ -f $(TAR_DIR)/VERSION ] && rm $(TAR_DIR)/VERSION || true
	[ -f $(TAR_DIR)/lambda.cfg ] && rm $(TAR_DIR)/lambda.cfg || true
	[ -f $(TAR_DIR)/config_info.py ] && rm $(TAR_DIR)/config_info.py || true
	[ -f $(TAR_DIR)/shared_utils.py ] && rm $(TAR_DIR)/shared_utils.py || true

	# Hard-coded benefits file:
	[ -f $(TAR_DIR)/benefits.py ] && rm $(TAR_DIR)/benefits.py || true

