#!/bin/sh
'''exec' "/Users/alejandroalva/Documents/Maestria/Computo Distribuido/proyecto/.venv/bin/python3.11" "$0" "$@"
' '''
# -*- coding: utf-8 -*-
import re
import sys
from rpyc.cli.rpyc_classic import main
if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(main())
