<?php

declare(strict_types=1);

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
            return true;
        } else {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        
    }
    
    /**
     * Return connection
     * 
     * @return type
     */
    public function getConnection() {
        return $this->_connection;
    }

    public function testConnection() {
        $server_info = sqlsrv_server_info($this->_connection);
        if( $server_info )
        {
            return server_info;
        } else {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        
    }
    
    /**
     * Return list of commands
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListCommand(int $limit = 10) {
        $sql = "SELECT TOP (?) * FROM Command";
        $stmt = sqlsrv_query($this->_connection, $sql, [$limit]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Return a list of PacketHeader rows
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListPacketHeader(int $limit = 10) {
        $sql = "SELECT TOP (?) * FROM PacketHeader";
        $stmt = sqlsrv_query($this->_connection, $sql, [$limit]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Return a list of PacketTable rows
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListPacketTable(int $limit = 10) {
        $sql = "SELECT TOP (?) * FROM PacketTable";
        $stmt = sqlsrv_query($this->_connection, $sql, [$limit]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Return a list of PacketTblLines rows
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListPacketTblLines(int $limit = 10) {
        $sql = "SELECT TOP (?) * FROM PacketTblLines";
        $stmt = sqlsrv_query($this->_connection, $sql, [$limit]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Return a list of Data rows
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListData(int $limit = 10) {
        $sql = "SELECT TOP (?) * FROM Data";
        $stmt = sqlsrv_query($this->_connection, $sql, [$limit]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Get a new command with field "Address" = $address
     * 
     * @param string $address
     * @return boolean|array
     */
    public function getNewCommandByAddress(string $address) {
        $sql = "SELECT TOP 1 Sender, ServiceModule, Command, id, InPacket
FROM Command
WHERE Completed = 'N' AND
      Status    = 'R' AND
      Address   = (?)
ORDER BY Priority DESC,
      id          ASC";
        $stmt = sqlsrv_query($this->_connection, $sql, [$address]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Get a awaiting response command with field "Sender" = $address
     * 
     * @param string $address
     * @return boolean|array
     */
    public function getAwaitResponseCommandByAddress(string $address) {
        $sql = "SELECT TOP 1
   id, Command, Status, ServiceModule,
   COALESCE(Comment,   '') AS Comment,
   COALESCE(OutPacket, -1) AS OutPacket
FROM Command
WHERE
   Completed =  'N'       AND
   Sender    =  (?) AND
   Status    IN ('E','S')
ORDER BY Completed, Sender, Status,
         Priority DESC,
         id       ASC";
        $stmt = sqlsrv_query($this->_connection, $sql, [$address]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Get dataId by packetId
     * 
     * @param int $packetId
     * @return boolean|array
     */
    public function getPacketHeaderByPacketId(int $packetId) {
        $sql = "SELECT DataId FROM PacketHeader WHERE PacketId = (?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$packetId]);
        return $this->fetchData($stmt);
    }
    
    /**
     * Return a list of PacketTable rows by packetId
     * 
     * @param int $limit
     * @return boolean|array
     */
    public function getListPacketTableByPacketId(int $packetId) {
        $sql = "SELECT TableId, TableName FROM PacketTable WHERE PacketId = (?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$packetId]);
        return $this->fetchData($stmt);
    }

    /**
     * Get a list of PacketTblLines rows by packetId and tableName
     * 
     * @param int $address
     * @return boolean|array
     */
    public function getPacketTblLinesByPacketIdAndTableName(int $packetId, string $tableName) {
        $sql = "SELECT DataId, LineNum
FROM PacketTblLines
JOIN PacketTable ON PacketTable.TableId = PacketTblLines.TableId
WHERE
   PacketId  =  (?)  AND
   TableName =  (?)
ORDER BY LineNum ASC";
        $stmt = sqlsrv_query($this->_connection, $sql, [$packetId, $tableName]);
        return $this->fetchData($stmt);
    }

    /**
     * Get a data by dataId
     * 
     * @param int $dataId
     * @return boolean|array
     */
    public function getDataByDataId(int $dataId) {
        $sql = "SELECT Field, Value FROM Data WHERE DataId = (?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$dataId]);
        return $this->fetchData($stmt);
    }
    
    /**
     * 
     * @param type $headerData
     * @param type $bodyData
     * @return type
     */
    public function createPacket($headerData, $bodyData) {
        $packetKey = intval($this->createTableKey('packet'));
        $dataKeyHeader = intval($this->createTableKey('data'));
        foreach ($headerData as $item) {
            $this->createDataRow($dataKeyHeader, strval($item['field']), strval($item['value']));
        }
        $this->createPacketHeader($packetKey, $dataKeyHeader);
        foreach ($bodyData as $line => $table) {
            $packetTableKey = intval($this->createPacketTable($packetKey, $table['tableName']));
            $dataKeyBody = intval($this->createTableKey('data'));
            foreach ($table['values'] as $value) {
                $this->createDataRow($dataKeyBody, strval($value['field']), strval($value['value']));
            }
            $this->createPacketTblLinesRow($packetTableKey, $line, $dataKeyBody);
        }
        return $packetKey;
    }
    
    /**
     * 
     * @param array $command
     * @return boolean
     */
    public function createCommand(array $command) {
        setlocale(LC_TIME, "ru_Ru");
        $data = strftime("%b %d %Y %H:%M");
        $sql = "INSERT INTO
Command( Command, ServiceModule, Priority, Sender, Address, DateCreated, Status, Completed, InPacket )
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        $stmt = sqlsrv_query(
            $this->_connection,
            $sql,
            [
                $command['command'],
                'W',
                $command['priority'] ?? 0,
                $command['sender'],
                $command['address'],
                $data,
                $command['status'] ?? 'R',
                $command['completed'] ?? 'N',
                $command['in_packet'],
            ]
        );
        if($stmt === false) {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return true;
    }
    
    /**
     * 
     * @param string $table
     * @return boolean
     */
    protected function createTableKey(string$table) {
        if ($table == 'data') {
            $sql = "INSERT INTO DataKey( Dummy ) VALUES('Y')
SELECT @@IDENTITY";
        } elseif ($table == 'packet') {
            $sql = "INSERT INTO PacketKey( Dummy ) VALUES('Y')
SELECT @@IDENTITY";
        }
        
        $stmt = sqlsrv_query($this->_connection, $sql);
        $next_result = sqlsrv_next_result($stmt);
        if($next_result) {
            $result = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_NUMERIC);
            sqlsrv_free_stmt($stmt);
            return $result[0];
        } elseif( is_null($next_result)) {
            ConnectionException::connectionFailed('MSSQL did not return id');
            sqlsrv_free_stmt($stmt);
            return false;
        } else {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return $this->fetchData($stmt);
    }
    
    /**
     * 
     * @param int $dataId
     * @param type $field
     * @param type $value
     * @return boolean
     */
    protected function createDataRow(int $dataId, string $field, string $value) {
        $sql = "INSERT INTO Data( DataId, Field, Value ) VALUES(?, ?, ?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$dataId, $field, $value]);
        if($stmt === false) {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return true;
    }
    
    /**
     * 
     * @param int $packetId
     * @param int $dataId
     * @return boolean
     */
    protected function createPacketHeader(int $packetId, int $dataId) {
        $sql = "INSERT INTO PacketHeader( PacketId, DataId ) VALUES(?, ?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$packetId, $dataId]);
        if($stmt === false) {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return true;
    }
    
    /**
     * 
     * @param int $packetId
     * @param string $tableName
     * @return boolean
     */
    protected function createPacketTable(int $packetId, string $tableName) {
        $sql = "INSERT INTO PacketTable(PacketId, TableName)
VALUES(?, ?)
SELECT @@IDENTITY";
        
        $stmt = sqlsrv_query($this->_connection, $sql, [$packetId, $tableName]);
        $next_result = sqlsrv_next_result($stmt);
        if($next_result) {
            $result = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_NUMERIC);
            sqlsrv_free_stmt($stmt);
            return $result[0];
        } elseif( is_null($next_result)) {
            ConnectionException::connectionFailed('MSSQL did not return id');
            sqlsrv_free_stmt($stmt);
            return false;
        } else {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return $this->fetchData($stmt);
    }
    
    /**
     * 
     * @param int $tableId
     * @param int $line
     * @param int $dataId
     * @return boolean
     */
    protected function createPacketTblLinesRow(int $tableId, int $line, int $dataId) {
        $sql = "INSERT INTO PacketTblLines(TableId, LineNum, DataId) VALUES(?, ?, ?)";
        $stmt = sqlsrv_query($this->_connection, $sql, [$tableId, $line, $dataId]);
        if($stmt === false) {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        }
        return true;
    }

    /**
     * Close connection
     */
    public function close() {
        sqlsrv_close($this->_connection);
    }
    
    /**
     * 
     * @param type $stmt
     * @return boolean|array
     */
    protected function fetchData($stmt) {
        if($stmt === false) {
            ConnectionException::connectionFailed($this->getError(sqlsrv_errors()));
            return false;
        } else {
            $result = [];
            while($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)) {
                $result[] = $row;
            }
            sqlsrv_free_stmt($stmt);
            return $result;
        }
    }

    /**
     * 
     * @param array $error
     * @return array
     */
    protected function getError(array $error) {
        $res = '';
        foreach ($error as $e) {
            $res .= $e['message'];
        }
        return $res;
    }
}

