[![Codacy Badge](https://api.codacy.com/project/badge/Grade/241637ab2c98404185f3eb76b90bf79a)](https://www.codacy.com?utm_source=bitbucket.org&amp;utm_medium=referral&amp;utm_content=matrixiangroup_dev/common_classes_mx&amp;utm_campaign=Badge_Grade)
## Python utilities for Matrixian Group's Data Team  
Read the [documentation on Confluence](https://matrixiangroup.atlassian.net/wiki/spaces/DBR/pages/1584693297/common+classes+mx).  

This package includes `common.connectors`: `MySQLClient`, `MongoDB`, `ESClient`,
and an `EmailClient`. Furthermore, they include an address parser
`common.api.address.parse`, plotting classes in `common.visualizations`,
`PersonData` and `NamesData` classes in `common.persondata`, `csv_read` and
`csv_write` functions in `common.handlers`, and much more. Enjoy!
## Installation  
```
COMMON='"git+ssh://git@bitbucket.org/matrixiangroup_dev/common_classes_mx.git#egg=common_classes_mx"'

# Install with all dependencies
pip install "$COMMON"[all]

# Install with some dependencies
pip install "$COMMON"[connectors]

# Install without dependencies
pip install "$COMMON"

# Update
pip install --upgrade "$COMMON"
``` 
© _Matrixian Group, created by Peter Saalbrink_
