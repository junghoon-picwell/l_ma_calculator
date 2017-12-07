HERE=`pwd`
LIB_DIR="$(HERE)/lib"
OUTPUT="$(HERE)/l_ma_calculator.zip"

# http://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html
package : clean
	pip install -r requirements.txt -t $(LIB_DIR)
	zip $(OUTPUT) *.py $(LIB_DIR)/*

clean :
	[ -d $(LIB_DIR) ] && rm -rf $(LIB_DIR) && (rm $(OUTPUT) || true) || true
