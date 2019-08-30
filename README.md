These are classes for MySQL, MongoDB, ElasticSearch, and an EmailClient.  

Install \[and upgrade\] using:  
`pip install [--upgrade] git+ssh://git@bitbucket.org/matrixiangroup_dev/common_classes_peter.git#egg=common`  

# To use these classes, create  
*  A directory called `'~/common_classes_peter/common/certificates/'` containing three `*.pem` files for MySQL connection  
*  A Python file called `'~/common_classes_peter/common/secrets.py'` containing four base64-encoded passwords for connections  

Format for `secrets.py`:  

`from base64 import b64decode`  
`mongo = ("devpsaalbrink", b"")`  
`sql = ("trainee_peter", b"")`  
`es = ("psaalbrink@matrixiangroup.com", b"")`  
`mail_pass = b"TmtUZ01wbThvVDNjSzk1NA=="`  
etc.

© _Matrixian Group, created by Peter Saalbrink._
