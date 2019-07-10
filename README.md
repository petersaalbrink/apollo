These are classes for MySQL, MongoDB, ElasticSearch, and an EmailClient.  
# To use these classes, create:  
* A directory called `'~/Python/common/certificates/'` containing three `*.pem` files for MySQL connection  
* A Python file called `'~/Python/common/secrets.py'` containing four base64-encoded passwords for connections  



Format for `secrets.py`:  

`from base64 import b64decode`  
`mongo = ("devpsaalbrink", b"")`  
`sql = ("trainee_peter", b"")`  
`es = ("psaalbrink@matrixiangroup.com", b"")`  
`mail_pass = b"TmtUZ01wbThvVDNjSzk1NA=="`  



© _Matrixian Group, created by Peter Saalbrink._
