<?php

/**
 * CodeIgniter
 *
 * An open source application development framework for PHP
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014-2019 British Columbia Institute of Technology
 * Copyright (c) 2019-2020 CodeIgniter Foundation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package    CodeIgniter
 * @author     CodeIgniter Dev Team
 * @copyright  2019-2020 CodeIgniter Foundation
 * @license    https://opensource.org/licenses/MIT	MIT License
 * @link       https://codeigniter.com
 * @since      Version 4.0.0
 * @filesource
 */

namespace CodeIgniter\Database\Informix;

use CodeIgniter\Database\BaseConnection;
use CodeIgniter\Database\ConnectionInterface;
use CodeIgniter\Database\Exceptions\DatabaseException;

/**
 * Connection for Informix
 */
class Connection extends BaseConnection implements ConnectionInterface
{
	/**
	 * Database driver
	 *
	 * @var string
	 */
	public $DBDriver = 'Informix';
	/**
	 * DELETE hack flag
	 *
	 * Whether to use the Informix "delete hack" which allows the number
	 * of affected rows to be shown. Uses a preg_replace when enabled,
	 * adding a bit more processing to all queries.
	 *
	 * @var boolean
	 */
	public $deleteHack = true;
	// --------------------------------------------------------------------
	/**
	 * Identifier escape character
	 *
	 * @var string
	 */
	public $escapeChar = '';
	// --------------------------------------------------------------------
	/**
	 * Connect to the database.
	 *
	 * @param boolean $persistent
	 *
	 * @return mixed
	 * @throws \CodeIgniter\Database\Exceptions\DatabaseException
	 */
	public function connect(bool $persistent = false)
	{
		if (empty($this->DSN)) { $this->buildDSN(); }

		// verify informix if exists
		if (mb_strpos($this->DSN, 'informix:') === 0)
		{
			// $this->DSN = mb_substr($this->DSN, 9);
			// throw error must be informix.
		}

		$options = array(
			// \PDO::ATTR_AUTOCOMMIT => FALSE, Not Work... throw an error
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			// \PDO::ATTR_ERRMODE => \PDO::ERRMODE_SILENT, // Ignore all errors and continue...the execution
			// \PDO::ATTR_ERRMODE => \PDO::ERRMODE_WARNING,
            \PDO::ATTR_CASE => \PDO::CASE_NATURAL
		);

		try
		{
			$this->connID = new \PDO($this->DSN, $this->username, $this->password, $options);

			return $this->connID;
		}
		catch (\CodeIgniter\Database\Exceptions\DatabaseException $e)
		{
			// Clean sensitive information from errors.
			$msg = $e->getMessage();

			$msg = str_replace($this->username, '****', $msg);
			$msg = str_replace($this->password, '****', $msg);

			$msg .= $this->DSN;

			throw new \CodeIgniter\Database\Exceptions\DatabaseException($msg, (int)$e->getCode(), $e);
		}

		return false;
	}

	//--------------------------------------------------------------------

	/**
	 * Keep or establish the connection if no queries have been sent for
	 * a length of time exceeding the server's idle timeout.
	 *
	 * @return void
	 */
	public function reconnect()
	{
		$this->close();
		$this->initialize();
	}

	//--------------------------------------------------------------------

	/**
	 * Close the database connection.
	 *
	 * @return void
	 */
	protected function _close()
	{
		$this->connID->close();
	}

	//--------------------------------------------------------------------

	/**
	 * Select a specific database table to use.
	 *
	 * @param string $databaseName
	 *
	 * @return boolean
	 */
	public function setDatabase(string $databaseName): bool
	{
        if ($databaseName === null || $databaseName === '') {
            $databaseName = $this->database;
        }

        if (empty($this->connID)) {
            $this->initialize();
        }

        if ($this->execute('DATABASE ' . $this->_escapeString($databaseName))) {
            $this->database  = $databaseName;
            $this->dataCache = [];

            return true;
        }

        return false;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns a string containing the version of the database being used.
	 *
	 * @return string
	 */
	public function getVersion(): string
	{
		if (isset($this->dataCache['version']))
		{
			return $this->dataCache['version'];
		}

		if (empty($this->informix))
		{
			$this->initialize();
		}

        try
        {
            $sql = "SELECT DBINFO('version','full') as version
                FROM systables
                WHERE tabid = 1;";
            $query = $this->connID->query($sql);
            $row   = $query->getRowArray();
            $this->dataCache['version'] = $row['version'];
        }
        catch (\PDOException $e)
        {
            throw new DatabaseException(
                $this->connID->errorCode()." ".
                "Error getting informix version:\n" . $sql . "\nWith Error:\n".$e->getMessage(),
                (int)$e->getMessage(),
                $e
            );
            if ($this->DBDebug) {
                throw $e;
            }
        }

		return $this->dataCache['version'];
	}

	//--------------------------------------------------------------------

	/**
	 * Executes the query against the database.
	 *
	 * @param string $sql
	 *
	 * @return mixed
	 */
	public function execute(string $sql)
	{
		try
		{
			return $this->connID->query(
				$this->prepQuery($sql)
			);
		}
		catch (\PDOException $e)
		{
			throw new DatabaseException(
				$this->connID->errorCode()." ".
				"Failed to execute query:\n" . $sql . "\nWith Error:\n".$e->getMessage(),
				(int)$e->getMessage(),
				$e
			);
			//log_message('error', 'Informix Query : '.$e);
			if ($this->DBDebug) {
				throw $e;
			}
		}
		return false;
	}

	//--------------------------------------------------------------------

	/**
	 * Prep the query
	 *
	 * If needed, each database adapter can prep the query string
	 *
	 * @param string $sql an SQL query
	 *
	 * @return string
	 */
	protected function prepQuery(string $sql): string
	{
		// Informix_affected_rows() returns 0 for "DELETE FROM TABLE" queries. This hack
		// modifies the query so that it a proper number of affected rows is returned.
		if ($this->deleteHack === true && preg_match('/^\s*DELETE\s+FROM\s+(\S+)\s*$/i', $sql))
		{
			return trim($sql) . ' WHERE 1=1';
		}

		//log_message('error', 'Prep: '.$sql);

		return $sql;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns the total number of rows affected by this query.
	 *
	 * @return integer
	 */
	public function affectedRows(): int
	{
		//log_message('error', 'affectedRows');
		return is_object($this->resultID) ? $this->resultID->rowCount() : 0;
	}

	//--------------------------------------------------------------------

	/**
	 * Platform-dependant string escape
	 *
	 * @param  string $str
	 * @return string
	 */
	protected function _escapeString(string $str): string
	{
		return str_replace("'", "''", remove_invisible_characters($str, false));
	}

	//--------------------------------------------------------------------

	/**
	 * Escape Like String Direct
	 * There are a few instances where Informix queries cannot take the
	 * additional "ESCAPE x" parameter for specifying the escape character
	 * in "LIKE" strings, and this handles those directly with a backslash.
	 *
	 * @param  string|string[] $str Input string
	 * @return string|string[]
	 */
	public function escapeLikeStringDirect($str)
	{
		//log_message('error', 'escapeLikeStringDirect');

		if (is_array($str))
		{
			foreach ($str as $key => $val)
			{
				$str[$key] = $this->escapeLikeStringDirect($val);
			}

			return $str;
		}

		$str = $this->_escapeString($str);

		// Escape LIKE condition wildcards
		return str_replace([
			$this->likeEscapeChar,
			'%',
			'_',
		], [
			'\\' . $this->likeEscapeChar,
			'\\' . '%',
			'\\' . '_',
		], $str
		);
	}

	//--------------------------------------------------------------------

	/**
	 * Generates the SQL for listing tables in a platform-dependent manner.
	 * Uses escapeLikeStringDirect().
	 *
	 * @param boolean $prefixLimit
	 *
	 * @return string
	 */
	protected function _listTables(bool $prefixLimit = false, ?string $tableName = null): string
	{

        $sql = 'SELECT "tabname" FROM "systables"
			WHERE "tabid" > 99 AND "tabtype" = \'T\' AND LOWER("owner") = '.$this->escape(strtolower($this->username));

        if ($prefixLimit !== false && $this->DBPrefix !== '')
        {
            $sql .= ' AND "tabname" LIKE \''.$this->escapeLikeStringDirect($this->dbprefix)."%' "
                .sprintf($this->_like_escape_str, $this->_like_escape_chr);
        }

		return $sql;
	}

	//--------------------------------------------------------------------

	/**
	 * Generates a platform-specific query string so that the column names can be fetched.
	 *
	 * @param string $table
	 *
	 * @return string
	 */
	protected function _listColumns(string $table = ''): string
	{
        if (strpos($table, '.') !== FALSE) {
            sscanf($table, '%[^.].%s', $owner, $table);
        } else {
            $owner = $this->username;
        }
        //$table = $this->protectIdentifiers($table, true, null, false);

        $table_columns = 'SELECT TRIM(colname) AS name 
                            CASE 
                                WHEN MOD(c.coltype, 256) = 0 THEN \'CHAR\' 
                                WHEN MOD(c.coltype, 256) = 1 THEN \'SMALLINT\' 
                                WHEN MOD(c.coltype, 256) = 2 THEN \'INTEGER\' 
                                WHEN MOD(c.coltype, 256) = 3 THEN \'FLOAT\' 
                                WHEN MOD(c.coltype, 256) = 4 THEN \'SMALLFLOAT\' 
                                WHEN MOD(c.coltype, 256) = 5 THEN \'DECIMAL\' 
                                WHEN MOD(c.coltype, 256) = 6 THEN \'SERIAL\' 
                                WHEN MOD(c.coltype, 256) = 7 THEN \'DATE\' 
                                WHEN MOD(c.coltype, 256) = 8 THEN \'MONEY\' 
                                WHEN MOD(c.coltype, 256) = 9 THEN \'NULL\' 
                                WHEN MOD(c.coltype, 256) = 10 THEN \'DATETIME\' 
                                WHEN MOD(c.coltype, 256) = 11 THEN \'BYTE\' 
                                WHEN MOD(c.coltype, 256) = 12 THEN \'TEXT\' 
                                WHEN MOD(c.coltype, 256) = 13 THEN \'VARCHAR\' 
                                WHEN MOD(c.coltype, 256) = 14 THEN \'INTERVAL\' 
                                WHEN MOD(c.coltype, 256) = 15 THEN \'NCHAR\' 
                                WHEN MOD(c.coltype, 256) = 16 THEN \'NVARCHAR\' 
                                WHEN MOD(c.coltype, 256) = 17 THEN \'INT8\' 
                                WHEN MOD(c.coltype, 256) = 18 THEN \'SERIAL8\' 
                                WHEN MOD(c.coltype, 256) = 19 THEN \'SET\' 
                                WHEN MOD(c.coltype, 256) = 20 THEN \'MULTISET\' 
                                WHEN MOD(c.coltype, 256) = 21 THEN \'LIST\' 
                                WHEN MOD(c.coltype, 256) = 22 THEN \'ROW (unnamed)\' 
                                WHEN MOD(c.coltype, 256) = 23 THEN \'COLLECTION\' 
                                WHEN MOD(c.coltype, 256) = 40 THEN \'LVARCHAR fixed-length opaque types\' 
                                WHEN MOD(c.coltype, 256) = 41 THEN \'BLOB, BOOLEAN, CLOB variable-length opaque types\' 
                                WHEN MOD(c.coltype, 256) = 43 THEN \'LVARCHAR (client-side only)\' 
                                WHEN MOD(c.coltype, 256) = 45 THEN \'BOOLEAN\' 
                                WHEN MOD(c.coltype, 256) = 52 THEN \'BIGINT\' 
                                WHEN MOD(c.coltype, 256) = 53 THEN \'BIGSERIAL\' 
                                WHEN MOD(c.coltype, 256) = 2061 THEN \'IDSSECURITYLABEL\' 
                                WHEN MOD(c.coltype, 256) = 4118 THEN \'ROW (named)\' 
                            END as type 
                            FROM systables 
                            JOIN syscolumns ON systables.tabid = syscolumns.tabid 
                            WHERE systables.tabtype = \'T\' 
				{AND LOWER(systables.owner) = ' . $this->escape(strtolower($owner)) . '}
				AND LOWER(systables.tabname) = ' . $this->escape(strtolower($table));

		return $table_columns;

	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with field data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 */
	public function _fieldData(string $table): array
	{
		//log_message('error', '_fieldData');
		if (($query = $this->query( $this->_listColumns($table))) === false)
		{
			throw new DatabaseException(lang('Database.failGetFieldData'));
		}
		$query = $query->getResultObject();

		//log_message('error', print_r($query, true) );

		return $query;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with index data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 * @throws \LogicException
	 */
	public function _indexData(string $table): array
	{
		//log_message('error', '_indexData');

		$sql = 'SELECT 
                    f.indexname, 
                    trim(
                    TRAILING "," FROM 
                        nvl(c1.colname, "") || "," || nvl(c2.colname, "") || "," || nvl(c3.colname, "") || "," || nvl(c4.colname, "") || "," || 
                        nvl(c5.colname, "") || "," || nvl(c6.colname, "") || "," || nvl(c7.colname, "") || "," || nvl(c8.colname, "") || "," || 
                        nvl(c9.colname, "") || "," || nvl(c10.colname, "") || "," || nvl(c11.colname, "") || "," || nvl(c12.colname, "") || "," || 
                        nvl(c13.colname, "") || "," || nvl(c14.colname, "") || "," || nvl(c15.colname, "") || "," || nvl(c16.colname, "")
                    ) AS fieldname,
                    CASE 
                        WHEN i.idxtype = "U" THEN "UNIQUE" 
                        WHEN i.idxtype = "D" THEN "DUPLICATES ALLOWED"
                        WHEN i.idxtype = "G" THEN "NONBITMAP GENERALIZED-KEY INDEX"
                        WHEN i.idxtype = "g" THEN "BITMAP GENERALIZED-KEY INDEX"
                        WHEN i.idxtype = "u" THEN "UNIQUE, BITMAP"
                        WHEN i.idxtype = "d" THEN "NONUNIQUE, BITMAP"
                    END AS indextype
                FROM sysfragments f
                JOIN sysindexes i ON i.idxname = f.indexname
                JOIN systables t ON t.tabid = i.tabid
                JOIN syscolumns c1 ON c1.tabid = i.tabid  AND c1.colno = i.part1
                LEFT JOIN syscolumns c2 ON c2.tabid = i.tabid  AND c2.colno = i.part2
                LEFT JOIN syscolumns c3 ON c3.tabid = i.tabid  AND c3.colno = i.part3
                LEFT JOIN syscolumns c4 ON c4.tabid = i.tabid  AND c4.colno = i.part4
                LEFT JOIN syscolumns c5 ON c5.tabid = i.tabid  AND c5.colno = i.part5
                LEFT JOIN syscolumns c6 ON c6.tabid = i.tabid  AND c6.colno = i.part6
                LEFT JOIN syscolumns c7 ON c7.tabid = i.tabid  AND c7.colno = i.part7
                LEFT JOIN syscolumns c8 ON c8.tabid = i.tabid  AND c8.colno = i.part8
                LEFT JOIN syscolumns c9 ON c9.tabid = i.tabid  AND c9.colno = i.part9
                LEFT JOIN syscolumns c10 ON c10.tabid = i.tabid  AND c10.colno = i.part10
                LEFT JOIN syscolumns c11 ON c11.tabid = i.tabid  AND c11.colno = i.part11
                LEFT JOIN syscolumns c12 ON c12.tabid = i.tabid  AND c12.colno = i.part12
                LEFT JOIN syscolumns c13 ON c13.tabid = i.tabid  AND c13.colno = i.part13
                LEFT JOIN syscolumns c14 ON c14.tabid = i.tabid  AND c14.colno = i.part14
                LEFT JOIN syscolumns c15 ON c15.tabid = i.tabid  AND c15.colno = i.part15
                LEFT JOIN syscolumns c16 ON c16.tabid = i.tabid  AND c16.colno = i.part16
                WHERE t.tabname = "' . $table . '"';

		if (($query = $this->query($sql)) === false)
		{
			throw new DatabaseException(lang('Database.failGetIndexData'));
		}
		$query = $query->getResultObject();

		return $query;

	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with Foreign key data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 */
	public function _foreignKeyData(string $table): array
	{
		//log_message('error', '_foreignKeyData');

		$sql = 'SELECT
            sc.constrname AS constraint_name, 
            st.tabname AS table_name, 
            scol.colname AS column_name, 
            fk_sa.tabname AS foreign_table_name,
            fk_scol.colname AS foreign_column_name,
            CASE 
                WHEN sr.delrule = \'C\' THEN \'Cascading Delete\'
                WHEN sr.delrule = \'R\' THEN \'Restrict\'
            END AS delete_rule,
            sr.updrule as update_rule,
            sr.matchtype as match_option
        FROM systables st 
        JOIN sysconstraints sc ON sc.tabid = st.tabid
        /*LEFT*/ JOIN sysreferences sr ON sr.constrid = sc.constrid
        JOIN sysindexes si ON si.idxname = sc.idxname
        JOIN syscolumns scol ON scol.tabid = st.tabid
        JOIN systables fk_sa ON fk_sa.tabid = sr.ptabid
        JOIN sysconstraints fk_sc ON fk_sc.constrid = sr.PRIMARY
        JOIN sysindexes fk_si ON fk_si.tabid = fk_sc.tabid AND fk_si.idxname = fk_sc.idxname
        JOIN syscolumns fk_scol ON fk_scol.tabid = fk_sc.tabid AND fk_scol.colno = fk_si.part1
        WHERE st.tabname = \''.$table.'\' AND sc.constrtype = \'R\' AND 
        (
            scol.colno = si.part1 or scol.colno = si.part2 or scol.colno = si.part3 or scol.colno = si.part4 or 
            scol.colno = si.part5 or scol.colno = si.part6 or scol.colno = si.part7 or scol.colno = si.part8 or 
            scol.colno = si.part9 or scol.colno = si.part10 or scol.colno = si.part11 or scol.colno = si.part12 or
            scol.colno = si.part13 or scol.colno = si.part14 or scol.colno = si.part15 or scol.colno = si.part16
        )';

		if (! empty($table)) $sql .= 'AND RDB$RELATION_NAME = \''.$table.'\' ';

		$sql .='ORDER BY scol.colname';

		//log_message('error', '_indexData SQL:'.$sql);

		if (($query = $this->query($sql)) === false)
		{
			throw new DatabaseException(lang('Database.failGetIndexData'));
		}
		$query = $query->getResultObject();

		//log_message('error', print_r($query, true) );

		return $query;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns platform-specific SQL to disable foreign key checks.
	 *
	 * @return string
	 */
	protected function _disableForeignKeyChecks()
	{
		return '';
	}

	//--------------------------------------------------------------------

	/**
	 * Returns platform-specific SQL to enable foreign key checks.
	 *
	 * @return string
	 */
	protected function _enableForeignKeyChecks()
	{
		return '';
	}

	//--------------------------------------------------------------------

	/**
	 * Returns the last error code and message.
	 *
	 * Must return an array with keys 'code' and 'message':
	 *
	 *  return ['code' => null, 'message' => null);
	 *
	 * @return array
	 */
	public function error(): array
	{
		if (! empty($this->connID->connect_errno))
		{
			return [
				'code'    => $this->connID->connect_errno,
				'message' => $this->connID->connect_error,
			];
		}

		return [
			'code'    => $this->connID->errno,
			'message' => $this->connID->error,
		];
	}

	//--------------------------------------------------------------------

	/**
	 * Build a DSN from the provided parameters
	 *
	 * @return void
	 */
	protected function buildDSN()
	{
		$DSN2 = 'informix:';

		$this->hostname === '' || $DSN2 .= "dbname={$this->hostname}";

		if (! empty($this->port) && ctype_digit($this->port))
		{
			$DSN2 .= ":{$this->port}";
		}

		$this->database === '' || $DSN2 .= ":{$this->database}";

		if (! empty($this->charset))
		{
			$DSN2 .= ";charset:{$this->charset}";
		}

		$this->DSN = rtrim($DSN2);
	}

	//--------------------------------------------------------------------

	/**
	 * Insert ID
	 *
	 * @return integer
	 */
	public function insertID($name = null): int
	{
        $sql = 'SELECT dbinfo(\'sqlca.sqlerrd1\') sc, dbinfo(\'serial8\') s, dbinfo(\'bigserial\') bs FROM systables WHERE tabid = 1;';

		//log_message('error', '_indexData SQL:'.$sql);

		if (($query = $this->query($sql)) === false)
        {
            throw new DatabaseException(lang('Database.failGetIndexData'));
        }
        $row = $query->getRowArray();

        if($row['sc'] > 0){
            $result = $row['sc'];
        }
        if($row['s'] > 0){
            $result = $row['s'];
        }
        if($row['bs'] > 0){
            $result = $row['bs'];
        }

		//log_message('error', print_r($query, true) );

		return $result;
		//return $this->connID->lastInsertId($name);
	}

	//--------------------------------------------------------------------

	/**
	 * Begin Transaction
	 *
	 * @return boolean
	 */
	protected function _transBegin(): bool
	{
		$this->connID->autocommit(false);

		return $this->connID->beginTransaction();
	}

	//--------------------------------------------------------------------

	/**
	 * Commit Transaction
	 *
	 * @return boolean
	 */
	protected function _transCommit(): bool
	{
		if ($this->connID->commit())
		{
			$this->connID->autocommit(true);

			return true;
		}

		return false;
	}

	//--------------------------------------------------------------------

	/**
	 * Rollback Transaction
	 *
	 * @return boolean
	 */
	protected function _transRollback(): bool
	{
		if ($this->connID->rollBack())
		{
			$this->connID->autocommit(true);

			return true;
		}

		return false;
	}
	//--------------------------------------------------------------------
}
