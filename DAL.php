<?php

/**
 * @Author: Juan Pablo Guereca.
 * @License: The MIT License.
 * @Description:
 * - Database Abstraction Layer
 * - Prepared statements with the method execute, wrapers start with execute_
 * - Queries using sprintf params binding with the method query, wrappers start with query_
 * - Queries without params binding with the method sql, wrappers start with sql_
 * - All the exceptions are children of DBError
 * - Dependant on the globals:
		const DB_USER = 'root';
		const DB_PASSWORD = '';
		const DB_CONNECT_TIMEOUT = 5;
		$DB = array(
			'main' => array(
					'db_name' => 'main',
					'host' => 'localhost',
					),
		);
	- All the queries are in the namespace "sql" in constants.
	- String's placeholders for emulated prepared statements need the quotes.
	- Prepared statements placeholders:
		- s: string.
		- d: integer.
		- f: float.
	- Emulated prepared statements placeholders:
		- a: array.
		- s: string.
		- d: integer.
		- f: float.
	@Todo: Support for arrays params in prepared statements.
 */

require 'sql.php';

class DBError extends Exception { }
class DBErrorConnecting extends DBError { }
class DBErrorExecutingStatement extends DBError { }
class DBErrorRunningQuery extends DBError { }
class DBErrorPreparingStatement extends DBError { }
class DBErrorInvalidQueryType extends DBError { }
class DBErrorCommiting extends DBError { }

class DAL {
	const CONNECT_TIMEOUT = \DB_CONNECT_TIMEOUT;
	
	const ERROR_PREPARING_STATEMENT = 'Error preparing statement: %s, error: %s, query: %s';
	const ERROR_EXECUTING_STATEMENT = 'Error executing statement: %s, %s';
	const ERROR_CONNECTING_TO_DB = 'Error connecting to db host: %s, db name: %s, error: %s';
	const ERROR_INVALID_QUERY_TYPE = 'Queries must start with SELECT, UPDATE, DELETE or INSERT not "%s"';
	const ERROR_RUNNING_QUERY = 'Error running query: %s, %s';
	
	const REGEX_PLACEHOLDERS_REPLACE = '#([^%])(%[asdfb])(")?#';
	const REGEX_PLACEHOLDERS_MATCH = '#[^%]%([asdfb])#';
	
	private static $db_instances = array();
	private static $statements = array();
	private static $statements_bind_result_params = array();
	private static $statements_stats = array();
	
	//Locking the construct to avoid instancing the class
	private function __construct() {}
	
	/**
	 * Singleton for the db instances.
	 * @param string $cluster_name
	 * @return Mysqli object
	 * @throws DBErrorConnecting
	 */
	private static function get_db_instance( $cluster_name ) {
		$cluster = $GLOBALS['DB'][$cluster_name];
		$db_name = $cluster['db_name'];
		if ( ! isset( self::$db_instances[$cluster_name] ) ) {
			$db_con = new Mysqli();
			$db_con->real_connect( $cluster['host'], DB_USER, DB_PASSWORD, $db_name );
			if ( $db_con->thread_id === null ) {
				throw new DBErrorConnecting( sprintf( self::ERROR_CONNECTING_TO_DB, $cluster['host'], $db_name, $db_con->connect_error ) );
			}
			// If the my.cnf is ok it's not needed
			//$db_con->set_charset( 'utf8' );
			$db_con->options( MYSQLI_OPT_CONNECT_TIMEOUT, self::CONNECT_TIMEOUT );
			// Using native data types for integers and floats instead of a string.
			$db_con->options( MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true );
			self::$db_instances[$cluster_name] = $db_con;
			
		}
		return self::$db_instances[$cluster_name];
	}
	
	/**
	 * Prepares sql statement.
	 * @param Mysqli $db_instance
	 * @param string $statement_name
	 * @return Mysqli_stmt
	 * @throws DBErrorPreparingStatement
	 */
	private static function prepare_statement( Mysqli $db_instance, $statement_name ) {
		$query = self::convert_to_statement_format( constant( sprintf( '\sql\%s', $statement_name ) ) );
		$statement = $db_instance->prepare( $query );
		if ( $statement === false  ) {
			trigger_error( sprintf( self::ERROR_PREPARING_STATEMENT, $statement_name, $db_instance->error, $query ), E_USER_ERROR );
			throw new DBErrorPreparingStatement( sprintf( self::ERROR_PREPARING_STATEMENT, $statement_name, $db_instance->error, $query ) );
		}
		return $statement;
	}
	
	/**
	 * Singleton for the prepared statements
	 * @param string $cluster_name
	 * @param string $statement_name
	 * @return Mysqli_stmt
	 */
	private static function get_statement( $cluster_name, $statement_name ) {
		if ( ! isset( self::$statements[$cluster_name] ) ) {
			self::$statements[$cluster_name] = array();
		}
		if ( ! isset( self::$statements[$cluster_name][$statement_name] ) ) {
			self::$statements[$cluster_name][$statement_name] = self::prepare_statement( self::$db_instances[$cluster_name], $statement_name );
		}
		
		return self::$statements[$cluster_name][$statement_name];
	}
	
	/**
	 * Binds prepared statements params
	 * @param Mysqli_stmt $statement
	 * @param string $params_mask
	 * @param array $query_params
	 * @return null
	 */
	private static function bind_params( Mysqli_stmt $statement, $params_mask, array $query_params ) {
		  $params = array( $params_mask );
		  foreach ( $query_params as $key => $value ) {
		  		$var_name = 'bind_' . $key;
				${$var_name} = $value;
				$params[] = &${$var_name};	  	
		  }
		  $method = new ReflectionMethod( $statement, 'bind_param' );
		  $method->invokeArgs( $statement, $params );
	}
	
	/**
	 * Bind statements results to an array passed by reference
	 * @param Mysqli_stmt $statement
	 * @param array &$row
	 * @return null
	 */
	private static function bind_result( Mysqli_stmt $statement, array &$row ) {
	    $meta = $statement->result_metadata();
		$params = array();
	    while ( $field = $meta->fetch_field() ) {
	        $params[] = &$row[$field->name];
	    }
	    $method = new ReflectionMethod( $statement, 'bind_result' );
	    $method->invokeArgs( $statement, $params );
	}
	
	/**
	 * Binds query params.
	 * @param string $query_name
	 * @param array $params
	 * @return string
	 */
	private static function bind_query_params( $query_name, $params ) {
		$query_without_params = \constant( \sprintf( '\sql\%s', $query_name ) );
		$params_iterator = new \ArrayIterator( $params );
		$query_sql = \preg_replace_callback( self::REGEX_PLACEHOLDERS_REPLACE, function( $matches ) use( $params_iterator ) {
			// Remove the general match because we just want the submatches
			array_shift( $matches );
			$character_before = array_shift( $matches );
			$placeholder = substr( array_shift( $matches ), 1 ); // %(n?[asdfb])
			// Some times will be null
			$character_after = array_shift( $matches );
			$replacement = $params_iterator->current();
			$result = '';
			switch( $placeholder ) {
				// String
				case 's':
					if ( null === $replacement ) {
						$result = $character_before . 'NULL' . $character_after;
					} else {
						$result = $character_before . \DAL::escape_string( $replacement ) . $character_after;
					}
				break;
				// Integer
				case 'd':
					if ( null === $replacement ) {
						$result = $character_before . 'NULL' . $character_after;
					} else {
						$result = $character_before . (int) $replacement . $character_after;
					}
				break;
				// Float
				case 'f':
					if ( null === $replacement ) {
						$result = $character_before . 'NULL' . $character_after;
					} else {
						$result = $character_before . (float) $replacement . $character_after;
					}
				break;
				// Array
				case 'a':
					$result = $character_before . \DAL::convert_array_to_sql_list( $replacement ) . $character_after;
				break;
			}
			$params_iterator->next();
			return $result;
		} , $query_without_params );
		return $query_sql;
	}
	
	public static function convert_array_to_sql_list( $list, $inner_arrays = false ) {
		$sql_list =  \implode( ',', \array_map( function( $x ) use( &$inner_arrays ) {
			if ( \is_string( $x ) ) {
				return  '"'. \DAL::escape_string( $x ).'"';
			} elseif ( \is_array( $x ) ) {
				$inner_arrays = true;
				return \DAL::convert_array_to_sql_list( $x );
			} else if ( null === $x ) {
				return 'NULL';
			} else {
				return $x;
			}
		}, $list ) );
		if ( $inner_arrays ) {
			return $sql_list;
		} else {
			return '(' . $sql_list . ')';
		}
	}
	
	/**
	 * Escapes a string for sql user.
	 * @param string $str
	 * @return string
	 */
	public static function escape_string( $str ) {
		return strtr( $str,
									array(
										"\\" 	=> "\\\\",
										"\0" 	=> "\\0",
										"\n" 	=> "\\n",
										"\r"	=> "\\r",
										"\x1a" 	=> "\Z",
										"'" 	=> "\'",
										'"' 	=> '\"' ) );
	}
	
	/**
	 * Starts db transaction in a specific cluster
	 * @param string $cluster_name
	 * @return boolean
	 */
	public static function start_transaction( $cluster_name ) {
		return self::get_db_instance( $cluster_name )->autocommit(false);
	}
	
	/**
	 * Sets the autocommit to true, watch out it includes commit so if you need it rollback the transaction.
	 * @param string $cluster_name
	 * @return boolean
	 */
	public static function finish_transaction( $cluster_name ) {
		return self::get_db_instance( $cluster_name )->autocommit(true);
	}
	
	/**
	 * Commits and rollbacks if it fails
	 * @param $cluster_name
	 * @return true or false, depending on the success
	 */
	public static function commit( $cluster_name ) {
		$connection = self::get_db_instance( $cluster_name );
		if ( ! $connection->commit() ) {
			$connection->rollback();
			return false;
		}
		//It's import to stop the transaction else the following queries updates or deletes wont last.
		return true;
	}
	
	/**
	 * Rollbacks current transaction
	 * @param string $cluster_name
	 * @return boolean
	 */
	public static function rollback( $cluster_name ) {
		$connection = self::get_db_instance( $cluster_name );
		return $connection->rollback();
	}
	
	/**
	 * Executes a prepared statement
	 * @param string, cluster_name
	 * @param string statement_name
	 * @param params ( Variable amount of params )
	 * @return:
	 * 		- SELECTS : array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
					)
			- UPDATES, DELETE and REPLACE: integer affected rows.
			- SET, LOCK and UNLOCK: boolean
			All of them return false if there is an error.
	 * @throws DBErrorRunningQuery, DBErrorInvalidQueryType, DBErrorExecutingStatement
	 */
	public static function execute( /* (func_get_args) $cluster_name, $statement_name, $params = '' */ ) {
		$func_arguments = func_get_args();
		
		$cluster_name = array_shift( $func_arguments );
		$statement_name = array_shift( $func_arguments );
		$params = $func_arguments;
		
		$connection = self::get_db_instance( $cluster_name );
		$statement = self::get_statement( $cluster_name, $statement_name );
		$params_mask = self::get_query_params_mask( $statement_name );
		if ( '' !== $params_mask ) {
			self::bind_params( $statement, $params_mask, $params );
		}
		$statement_start_time = microtime(true);
		//Executing statement
		if ( false === $statement->execute( ) ) {
			throw new DBErrorExecutingStatement( sprintf( self::ERROR_EXECUTING_STATEMENT, $statement_name, $statement->error ) );
		}
		
		$query_type = substr( constant( sprintf( '\sql\%s', $statement_name ) ), 0, 6 );
		
		$num_of_rows = 0;
		
		//If it's a SELECT a result it's expected
		if ( 'SELECT' === $query_type ) {
			$statement->store_result();
			
			$row = array();

			$all_rows = array();
			while ( true ) {
				self::bind_result( $statement, $row );
				if ( null === $statement->fetch() ) {
					break;
				}
			    // Copy the row else all of them will be a reference to the last one
				$all_rows[] = array_map( function ( $x ) { return $x; }, $row );
			    ++$num_of_rows;
			}
		
			$total_rows = $num_of_rows;
			$query = constant( sprintf( '\sql\%s', $statement_name ) );
			$pos = strpos ( $query, 'SQL_CALC_FOUND_ROWS', 7 );
                        
			if ( $pos === 7 ) {
				if ( $connection->real_query( 'SELECT FOUND_ROWS()' ) ) {
					$result = new MySQLi_Result( $connection );
					$field = $result->fetch_row();
					$total_rows = $field[0]; 
				} else {
					throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
				}
			}

			$result = array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
						);
		} elseif ( 'INSERT' === $query_type ) {
			$result = $statement->insert_id;
		} elseif ( 
			'UPDATE' === $query_type ||
			'DELETE' === $query_type ||
			'REPLAC' === $query_type ||
			strpos( $query_type, 'LOAD ' ) === 0 ) {
			// If errno is 0, then all is ok else ko.
			$result = $connection->affected_rows;
		} elseif ( 
				strpos( $query_type, 'SET ' ) === 0 ||
				strpos( $query_type, 'LOCK' ) === 0 ||
				strpos( $query_type, 'UNLOCK' ) === 0 ||
				strpos( $query_type, 'CREATE') === 0 ||
				strpos( $query_type, 'DROP') === 0
				) {
			$result = true;
		} else {
			throw new DBErrorInvalidQueryType( sprintf( self::ERROR_INVALID_QUERY_TYPE, $query_type) );
		}
		if ( $statement->errno !== 0 ) {
			throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
		}
		$statement->free_result();
		
		$statement_exec_time = microtime(true) - $statement_start_time;
		self::record_stats( $statement_name, $statement_exec_time );
		
		return $result;
	}
	
	/**
	 * Converts sprintf placeholder format to prepared statements format
	 * and returns the params_mask
	 * @param string $query_sql
	 * @return string
	 */
	private static function get_query_params_mask( $statement_name ) {
		$query_sql = constant( sprintf( '\sql\%s', $statement_name ) );
		$matches = array();
		$params_mask = '';
		if ( 0 !== \preg_match_all( self::REGEX_PLACEHOLDERS_MATCH, $query_sql, $matches ) ) {
			foreach ( $matches[1] as $match ) {
				switch( $match ) {
					case 'd':
						$params_mask .= 'i';
					break;
					case 'f':
						$params_mask .= 'd';
					break;
					case 's':
						$params_mask .= 's';
					break;
				}
			}
		}
		return $params_mask;
	}

	/**
	 * Get a prepared statements params mask from sprintf placeholders.
	 * and returns the params_mask
	 * @param string $query_sql
	 * @return string
	 */
	private static function convert_to_statement_format( $query_sql ) {
		return \preg_replace( self::REGEX_PLACEHOLDERS_REPLACE, '$1?', $query_sql );
	}
	
	/**
	 * Records stats for a query
	 * @param string $query_name
	 * @param int $exec_time
	 * @return null
	 */
	private static function record_stats( $query_name, $exec_time) {
		// There are stats already.
		if ( array_key_exists( $query_name, self::$statements_stats ) ) {
			$current = self::$statements_stats[$query_name];
			self::$statements_stats[$query_name] = array(
				'counter' => ( $current['counter'] + 1 ),
				'total_time' => ( $current['total_time'] + $exec_time )
			);
		} else {
		// There aren't stats already.
			self::$statements_stats[$query_name] = array(
				'counter' => 1,
				'total_time' => $exec_time
			);
		}
	}
	
	/**
	 * Get mysql stats from the mysqlnd driver
	 * @return array
	 */
	public static function get_mysqlnd_stats() {
		$stats = array();
		foreach ( self::$db_instances as $cluster_name => $instance ) {
			$stats[$cluster_name] =  $instance->get_connection_stats();
		}
		return $stats;
	}
	
	/**
	 * Get benchmark from the statements run
	 * @return array
	 */
	public static function get_statements_stats() {
		return self::$statements_stats;
	}
	
	/**
	 * Runs sql queries which aren't prepared statements, params are bind with sprintf
	 * @param string $cluster_name
	 * @param string $query_name
	 * @param params ( Variable amount of params )
	 * @return:
	 * 		- SELECTS : array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
					)
			- UPDATES, DELETE and REPLACE: integer affected rows.
			- SET, LOCK and UNLOCK: boolean
			
			All of them return false if there is an error.
	 * @throws DBErrorRunningQuery, DBErrorInvalidQueryType
	 */
	public static function query( /* (func_get_args) $cluster_name, $query_name, $params ... */  ) {
		$func_arguments = \func_get_args();
		$cluster_name = \array_shift( $func_arguments );
		$query_name = \array_shift( $func_arguments );
		$params = $func_arguments;
		$connection = self::get_db_instance( $cluster_name );
		$query_sql = self::bind_query_params( $query_name, $params );
		$query_start_time = \microtime( true );
		//Executing statement
		if ( false === $connection->real_query( $query_sql ) ) {
			throw new DBErrorRunningQuery( \sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ));
		}
		$query_type = \substr( \constant( \sprintf( '\sql\%s', $query_name ) ), 0, 6 );
		$num_of_rows = 0;
		//If it's a SELECT a result it's expected
		if ( 'SELECT' === $query_type ) {
			if (  0 === $connection->field_count ) {
				$all_rows = array();
				$num_of_rows = 0;
			} else {
				$result = new \MySQLi_Result( $connection );
				$all_rows = $result->fetch_all( MYSQLI_ASSOC );
				$num_of_rows = $result->num_rows;
			}
			$total_rows = $num_of_rows;
			$pos = strpos( $query_sql, 'SQL_CALC_FOUND_ROWS', 7 );
			if ( $pos === 7 ) {
				if ( $connection->real_query( 'SELECT FOUND_ROWS()' ) ) {
					$result = new MySQLi_Result( $connection );
					$field = $result->fetch_row();
					$total_rows = $field[0];
				} else {
					throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
				}
			}
			$result = array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
						);
		} elseif ( 'INSERT' === $query_type ) {
			$result = $connection->insert_id;
		} elseif ( 
			'UPDATE' === $query_type ||
			'DELETE' === $query_type ||
			'REPLAC' === $query_type ||
			strpos( $query_type, 'LOAD ' ) === 0 ) {
			// If errno is 0, then all is ok else ko.
			$result = $connection->affected_rows;
		} elseif ( 
				strpos( $query_type, 'SET ' ) === 0 ||
				strpos( $query_type, 'LOCK' ) === 0 ||
				strpos( $query_type, 'UNLOCK' ) === 0 ||
				strpos( $query_type, 'CREATE') === 0 ||
				strpos( $query_type, 'DROP') === 0
				) {
			$result = true;
		} else {
			throw new DBErrorInvalidQueryType( sprintf( self::ERROR_INVALID_QUERY_TYPE, $query_type) );
		}
		if ( $connection->errno !== 0 ) {
			throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
		}

		$query_exec_time = microtime(true) - $query_start_time;
		self::record_stats( $query_name, $query_exec_time );
		
		return $result;
	}

	/**
	 * Runs sql queries withour any kind of param binding
	 * @param string $cluster_name
	 * @param string $query_sql
	 * @return:
	 * 		- SELECTS : array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
					)
			- UPDATES, DELETE and REPLACE: integer affected rows.
			- SET, LOCK and UNLOCK: boolean
			
			All of them return false if there is an error.
	 * @throws DBErrorRunningQuery, DBErrorInvalidQueryType
	 */
	public static function sql( $cluster_name, $query_sql ) {
		$connection = self::get_db_instance( $cluster_name );
		$query_start_time = microtime(true);
		//Executing statement
		if ( false === $connection->real_query( $query_sql ) ) {
			throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_sql, $connection->error ));
		}
		
		$query_type = substr( $query_sql, 0, 6 );
		
		$num_of_rows = 0;
		
		//If it's a SELECT a result it's expected
		if ( 'SELECT' === $query_type ) {
			$result = new MySQLi_Result( $connection );
			$all_rows = $result->fetch_all( MYSQLI_ASSOC );
			$num_of_rows = $result->num_rows;
			$total_rows = $num_of_rows;
			$pos = strpos ( $query_sql, 'SQL_CALC_FOUND_ROWS', 7 );
			                        
			if ( $pos === 7 ) {
				if ( $connection->real_query( 'SELECT FOUND_ROWS()' ) ) {
					$result = new MySQLi_Result( $connection );
					$field = $result->fetch_row();
					$total_rows = $field[0];
				} else {
					throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
				}
			}

			$result = array(
							'num' 	=> $num_of_rows,
							'rows'  => $all_rows,
							'total_rows'  => $total_rows
						);
		} elseif ( 'INSERT' === $query_type ) {
			$result = $connection->insert_id;
		} elseif ( 
			'UPDATE' === $query_type ||
			'DELETE' === $query_type ||
			'REPLAC' === $query_type ||
			strpos( $query_type, 'LOAD ' ) === 0 ) {
			// If errno is 0, then all is ok else ko.
			$result = $connection->affected_rows;
		} elseif ( 
				strpos( $query_type, 'SET ' ) === 0 ||
				strpos( $query_type, 'LOCK' ) === 0 ||
				strpos( $query_type, 'UNLOCK' ) === 0 ||
				strpos( $query_type, 'CREATE') === 0 ||
				strpos( $query_type, 'DROP') === 0
				) {
			$result = true;
		} else {
			throw new DBErrorInvalidQueryType( sprintf( self::ERROR_INVALID_QUERY_TYPE, $query_type) );
		}
		if ( $connection->errno !== 0 ) {
			throw new DBErrorRunningQuery( sprintf( self::ERROR_RUNNING_QUERY, $query_name, $connection->error ) );
		}

		$query_exec_time = microtime(true) - $query_start_time;
		self::$statements_stats[$query_sql] = $query_exec_time;
		
		return $result;
	}

	/**
	 * Wrapper for the method sql that expects a single column value as result
	 * @param string $cluster_name
	 * @param string $query_sql
	 * @return string or int or null if empty
	 */
	public static function sql_value( $cluster_name, $query_sql ) {
		$result = self::sql( $cluster_name, $query_sql );
		if ( $result['num'] === 0 ) {
			return null;
		}
		foreach ( $result['rows'][0] as $value ) {
			return $value;
		}
	}
	
	/**
	 * Wrapper for the method sql that expects a single row as result
	 * @param string $cluster_name
	 * @param string $query_sql
	 * @return array or null if empty
	 */
	public static function sql_row( $cluster_name, $query_sql ) {
		$result = self::sql( $cluster_name, $query_sql );
		if ( $result['num'] === 0 ) {
			return null;
		}
		return $result['rows'][0];
	}
	
	/** 
	 * Wrapper for the method sql that expects several rows as result
	 * @param string $cluster_name
	 * @param string $query_sql
	 * @return array
	 */
	public static function sql_rows( $cluster_name, $query_sql ) {
		$result = self::sql( $cluster_name, $query_sql );
		return $result['rows'];
	}
	
	/**
	 * Wrapper for the method execute that expects a single column value as result
	 * @param variable number of params
	 * @return string, int or null if empty
	 */
	public static function execute_value( /* (func_get_args) $cluster_name, $statement_name, $params ... */ ) {
		$method = new ReflectionMethod( __CLASS__, 'execute' );
		$result = $method->invokeArgs( null, func_get_args() );
		if ( $result['num'] === 0 ) {
			return null;
		}
		foreach ( $result['rows'][0] as $value ) {
			return $value;
		}
	}
	
	/**
	 * Wrapper for the method execute that expects a single row as result
	 * @param variable number of params
	 * @return array or null if empty
	 */
	public static function execute_row( /* (func_get_args) $cluster_name, $statement_name, $params ...  */ ) {
		$method = new ReflectionMethod( __CLASS__, 'execute' );
		$result = $method->invokeArgs( null, func_get_args() );
		if ( $result['num'] === 0 ) {
			return null;
		}
		return $result['rows'][0];
	}
	
	/**
	 * Wrapper for the method execute that exepcts several rows as result
	 * @param variable number of params
	 * @return array
	 */
	public static function execute_rows( /* (func_get_args) $cluster_name, $statement_name, $params ... */ ) {
		$method = new ReflectionMethod( __CLASS__, 'execute' );
		$result = $method->invokeArgs( null, func_get_args() );
		return $result['rows'];
	}
	
	/**
	 * Wrapper for the method query that exepctes a single column value as result
	 * @param variable number of params
	 * @return string, int or null if empty
	 */
	public static function query_value( /* (func_get_args) $cluster_name, $statement_name, $params ... */ ) {
		$method = new ReflectionMethod( __CLASS__, 'query' );
		$result = $method->invokeArgs( null, func_get_args() );
		if ( $result['num'] === 0 ) {
			return null;
		}
		foreach ( $result['rows'][0] as $value ) {
			return $value;
		}
	}
	
	/**
	 * Wrapper for the method query that expects a singel row as result
	 * @param variable number of params
	 * @return array or null if empty
	 */
	public static function query_row( /* (func_get_args) $cluster_name, $statement_name, $params ... */ ) {
		$method = new ReflectionMethod( __CLASS__, 'query' );
		$result = $method->invokeArgs( null, func_get_args() );
		if ( $result['num'] === 0 ) {
			return null;
		}
		return $result['rows'][0];
	}
	
	/**
	 * Wrapper for the method query that exepcts several rows as result
	 * @param variable number of params
	 * @return array
	 */
	public static function query_rows( /* (func_get_args) $cluster_name, $statement_name, $params ... */ ) {
		$method = new ReflectionMethod( __CLASS__, 'query' );
		$result = $method->invokeArgs( null, func_get_args() );
		return $result['rows'];
	}
}