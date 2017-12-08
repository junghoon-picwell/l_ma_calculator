.ONESHELL:

VERSION_FILE := VERSION
HERE := $(shell pwd)
VSN := $(shell cat ${VERSION_FILE})
OUTPUT := $(HERE)/build/l_ma_calculator-$(VSN).zip
PROJ_DIR := $(HERE)

# http://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
# And:
# http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python
ver :
	ECHO $(VSN)

	ECHO $(OUTPUT)

	ECHO $(VIRTUAL_ENV)

package : clean
	ECHO $(HERE)
	ECHO $(OUTPUT)
	pip install -r requirements.txt
	zip $(OUTPUT) *.py
	cd $(VIRTUAL_ENV)/lib/python2.7/site-packages && zip -r9 $(OUTPUT) ./* && cd $(HERE)
	cd $(VIRTUAL_ENV)/src/misscleo && zip -r9 $(OUTPUT) ./*
	#cd $(VIRTUAL_ENV)/src/etl && zip -r9 $(OUTPUT) ./*


clean :
	[ -d $(LIB_DIR) ] && rm -rf $(LIB_DIR) && (rm $(PROJ_DIR)/build/* || true) || true

