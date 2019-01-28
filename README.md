# PharmaScraper



## connect to tableau

1. download and install connector from https://www.mongodb.com/download-center/bi-connector?jmp=tbl
2. Update the installation directory in system path
3. Open command prompt
4. Navigate to the keys directory in the repo /Pharmascraper/Tableau/Keys
5. start mongosql using command ```mongosqld --mongo-uri mongodb://mongodb-712-0.cloudclusters.net:27017/?connect=direct --mongo-authenticationSource admin --auth --mongo-username < username > --mongo-password < password > --mongo-ssl --sslPEMKeyFile=mongosqld-server.pem --sslMode=requireSSL```
6. Open Tableau desktop fisrt and connect to " Mongodb BI  Connector"
7. Enter 127.0.0.1 as host . 3307 is default port (leave as is)\
8. enter username in this format < usename >?source=admin
9. enter password 
10. Check ssl box 
11. Then select ca.crt from the keys directory
11. Then connect
12. open tableau prep and follow above steps except 11 and connect