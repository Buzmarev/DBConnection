<?php

namespace Tagesjump\MSSQLConnection;

use Tagesjump\MSSQLConnection\Exception\ConnectionException;

class DBConnection
{
    protected $_connection;
    
    public function createConnection(
        string $serverName,
        string $dbName,
        string $uid,
        string $pwd
    ) {
        $conn = sqlsrv_connect(
            $serverName,
            [
                "Database" => $dbName,
                "UID" => $uid,
                "PWD" => $pwd
            ]
        );
        
        if ($conn) {
            $this->_connection = $conn;
        } else {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
        }
        
    }
    
    public function getConnection() {
        return $this->_connection;
    }

    public function testConnection() {
        $server_info = sqlsrv_server_info($this->_connection);
        if( $server_info )
        {
            foreach( $server_info as $key => $value) {
               echo $key.": ".$value."<br />";
            }
        } else {
              die( print_r( sqlsrv_errors(), true));
        }
    }
    
    public function close() {
        sqlsrv_close($this->_connection);
    }
    
    protected function getError(array $error) {
        $res = '';
        foreach ($error as $e) {
            $res .= $e['message'];
        }
        return $res;
    }
}

