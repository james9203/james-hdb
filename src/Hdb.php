<?php
/**
 * hana 数据库操作类
 * author: james9203@126.com
 * Date: 2019/6/28
 * Time: 16:05
 */

namespace jamesluo\hana;
class  Hdb
{
    private $username = '';
    private $password = '';
    private $servernode = '';
    private $conn = '';
    private $stmt = '';

    public function __construct($config)
    {
        $this->servernode = isset($config['servernode']) ? $config['servernode'] : '';
        $this->username = isset($config['username']) ? $config['username'] : '';
        $this->password = isset($config['password']) ? $config['password'] : '';
        $this->connect();
    }

    public function connect()
    {
        try {
            if (!function_exists('hdb_connect')) {
                throw new \Exception('undefined function hdb_connect(), 请加载hdb 模块');
            }
            $options = array("UID" => $this->username, "PWD" => $this->password);
            $this->conn = hdb_connect($this->servernode, $options);
            if ($this->conn == false) {
                throw  new \Exception($this->hdberror());
            }
        } catch (\Exception $e) {
            echo $e->getMessage();
            die;

        }
    }

    public function hdberror()
    {
        $error = hdb_errors();
        if (!empty($error)) {
            return json_encode($error);
        }
        return '未知hdb错误';
    }

    /**
     * 查询 返回数组
     * @param $sql
     * @return array
     */
    public function select($sql)
    {
        try {
            $this->stmt = hdb_query($this->conn, $sql);
            if ($this->stmt === false) {
                throw  new \Exception($this->hdberror());
            }
            $arr = array();
            while ($row = hdb_fetch_array($this->stmt, 2)) {
                $arr[] = $row;
            }
            hdb_free_stmt($this->stmt);
            return $arr;
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * 返回一条查询结果
     * @param $sql
     * @return array
     */
    public function selectOne($sql)
    {
        try {
            $this->stmt = hdb_query($this->conn, $sql);
            if ($this->stmt === false) {
                throw  new \Exception($this->hdberror());
            }
            $row = hdb_fetch_array($this->stmt, 2);
            hdb_free_stmt($this->stmt);
            if (!empty($row)) {
                return $row;
            }
            return array();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    public function parseValue($value)
    {
        if (is_string($value)) {
            $value = '\'' . $value . '\'';
        } elseif (isset($value[0]) && is_string($value[0]) && strtolower($value[0]) == 'exp') {
            $value = $value[1];
        } elseif (is_array($value)) {
            $value = array_map(array($this, 'parseValue'), $value);
        } elseif (is_null($value)) {
            $value = 'null';
        }
        return $value;
    }

    /**
     * 更新数据
     * @param $table
     * @param $where
     * @param array $set
     * @return bool
     */
    public function update($table, $where, $set = array())
    {
        try {
            foreach ($set as $key => $value) {
                $sets[] = "{$key} = " . $this->parseValue($value);
            }
            $sql = "UPDATE {$table} SET " . implode(',', $sets) . '  WHERE  ' . $where;
            $this->stmt = hdb_prepare($this->conn, $sql);
            if ($this->stmt!=false&&hdb_execute($this->stmt)) {
                hdb_free_stmt($this->stmt);
                return true;
            }
            return false;

        } catch (Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * 插入数据
     * @param string $table
     * @param array $set
     * @param boolean $replace 是否replace
     */
    public function insert($table, $set = array(), $replace = false)
    {
        try {
            $fields = array();
            $values = array();
            foreach ($set as $key => $value) {
                $fields[] = "{$key}";
                $values[] = $this->parseValue($value);
            }
            $sql = ($replace ? 'REPLACE' : 'INSERT') . " INTO {$table} " . ' (' . implode(',', $fields) . ') VALUES (' . implode(',', $values) . ')';
            $this->stmt = hdb_query($this->conn, $sql);
            if ($this->stmt === false) {
                throw new  Exception($this->hdberror());
            }
            hdb_free_stmt($this->stmt);
            return true;

        } catch (Exception $e) {
            echo $e->getMessage();
        }
    }


    public function delete($table, $where = '')
    {
        try {
            if (empty($where)) {
                return false;
            }
            $sql = "DELETE FROM {$table} WHERE ".$where;
            $this->stmt = hdb_prepare($this->conn, $sql);
            if ($this->stmt!=false&&hdb_execute($this->stmt)) {
                hdb_free_stmt($this->stmt);
                return true;
            }
        } catch (Exception $e) {
            echo $e->getMessage();
        }
    }

    public function __destruct()
    {
        hdb_close($this->conn);
    }
}
