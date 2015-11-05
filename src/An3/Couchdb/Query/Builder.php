<?php

namespace An3\Couchdb\Query;

use Closure;
use DateTime;
use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Database\Query\Expression;
use Illuminate\Support\Collection;
use An3\CouchDB\Connection;
use MongoDate;
use MongoId;
use MongoRegex;

class Builder extends BaseBuilder
{
    /**
     * [$documentName description].
     *
     * @var [type]
     */
    protected $documentName;

    /**
     * The database collection.
     *
     * @var MongoCollection
     */
    protected $collection;

    /**
     * The column projections.
     *
     * @var array
     */
    public $projections;

    /**
     * The cursor timeout value.
     *
     * @var int
     */
    public $timeout;

    /**
     * The cursor hint value.
     *
     * @var int
     */
    public $hint;

    /**
     * Indicate if we are executing a pagination query.
     *
     * @var bool
     */
    public $paginating = false;

    /**
     * All of the available clause operators.
     *
     * @var array
     */
    protected $operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=',
        'like', 'not like', 'between', 'ilike',
        '&', '|', '^', '<<', '>>',
        'rlike', 'regexp', 'not regexp',
        'exists', 'type', 'mod', 'where', 'all', 'size', 'regex', 'text', 'slice', 'elemmatch',
        'geowithin', 'geointersects', 'near', 'nearsphere', 'geometry',
        'maxdistance', 'center', 'centersphere', 'box', 'polygon', 'uniquedocs',
    ];

    /**
     * Operator conversion.
     *
     * @var array
     */
    protected $conversion = [
        '=' => '=',
        '!=' => '$ne',
        '<>' => '$ne',
        '<' => '$lt',
        '<=' => '$lte',
        '>' => '$gt',
        '>=' => '$gte',
    ];

    /**
     * Create a new query builder instance.
     *
     * @param Connection $connection
     */
    public function __construct($connection, $documentName)
    {
        $this->connection = $connection;
        $this->documentName = $documentName;
    }

    /**
     * Execute a query for a single record by ID.
     *
     * @param mixed $id
     * @param array $columns
     *
     * @return mixed
     */
    public function find($id, $columns = [])
    {
        return $this->connection->find($this->documentName, $id);
    }

    /**
     * Insert a new record into the database.
     *
     * @param array $values
     *
     * @return bool
     */
    public function insert(array $values)
    {
        // Since every insert gets treated like a batch insert, we will have to detect
        // if the user is inserting a single document or an array of documents.
        $batch = true;

        foreach ($values as $value) {
            // As soon as we find a value that is not an array we assume the user is
            // inserting a single document.
            if (!is_array($value)) {
                $batch = false;
                break;
            }
        }

        if (!$batch) {
            $values = [$values];
        }

        // Batch insert
        $result = $this->collection->batchInsert($values);

        return (1 == (int) $result['ok']);
    }

    /**
     * Insert a new record and get the value of the primary key.
     *
     * @param array  $values
     * @param string $sequence
     *
     * @return int
     */
    public function insertGetId(array $values, $sequence = null)
    {
        $result = $this->collection->insert($values);

        if (1 == (int) $result['ok']) {
            if (is_null($sequence)) {
                $sequence = '_id';
            }

            // Return id
            return $values[$sequence];
        }
    }

    /**
     * Update a record in the database.
     *
     * @param array $values
     * @param array $options
     *
     * @return int
     */
    public function update(array $values, array $options = [])
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Increment a column's value by a given amount.
     *
     * @param string $column
     * @param int    $amount
     * @param array  $extra
     *
     * @return int
     */
    public function increment($column, $amount = 1, array $extra = [], array $options = [])
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Decrement a column's value by a given amount.
     *
     * @param string $column
     * @param int    $amount
     * @param array  $extra
     *
     * @return int
     */
    public function decrement($column, $amount = 1, array $extra = [], array $options = [])
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Delete a record from the database.
     *
     * @param mixed $id
     *
     * @return int
     */
    public function delete($id = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Set the collection which the query is targeting.
     *
     * @param string $collection
     *
     * @return Builder
     */
    public function from($collection)
    {
        return $this->connection->getCollection($collection);
    }

    /**
     * Run a truncate statement on the table.
     */
    public function truncate()
    {
        $result = $this->collection->remove();

        return (1 == (int) $result['ok']);
    }

    /**
     * Get an array with the values of a given column.
     *
     * @param string $column
     * @param string $key
     *
     * @return array
     */
    public function lists($column, $key = null)
    {
        if ($key == '_id') {
            $results = new Collection($this->get([$column, $key]));

            // Convert MongoId's to strings so that lists can do its work.
            $results = $results->map(function ($item) {
                $item['_id'] = (string) $item['_id'];

                return $item;
            });

            return $results->lists($column, $key)->all();
        }

        return parent::lists($column, $key);
    }

    /**
     * Create a raw database expression.
     *
     * @param closure $expression
     *
     * @return mixed
     */
    public function raw($expression = null)
    {
        // Execute the closure on the mongodb collection
        if ($expression instanceof Closure) {
            return call_user_func($expression, $this->collection);
        }

        // Create an expression for the given value
        elseif (!is_null($expression)) {
            return new Expression($expression);
        }

        // Quick access to the mongodb collection
        return $this->collection;
    }

    /**
     * Append one or more values to an array.
     *
     * @param mixed $column
     * @param mixed $value
     *
     * @return int
     */
    public function push($column, $value = null, $unique = false)
    {
        // Use the addToSet operator in case we only want unique items.
        $operator = $unique ? '$addToSet' : '$push';

        // Check if we are pushing multiple values.
        $batch = (is_array($value) and array_keys($value) === range(0, count($value) - 1));

        if (is_array($column)) {
            $query = [$operator => $column];
        } elseif ($batch) {
            $query = [$operator => [$column => ['$each' => $value]]];
        } else {
            $query = [$operator => [$column => $value]];
        }

        return $this->performUpdate($query);
    }

    /**
     * Remove one or more values from an array.
     *
     * @param mixed $column
     * @param mixed $value
     *
     * @return int
     */
    public function pull($column, $value = null)
    {
        // Check if we passed an associative array.
        $batch = (is_array($value) and array_keys($value) === range(0, count($value) - 1));

        // If we are pulling multiple values, we need to use $pullAll.
        $operator = $batch ? '$pullAll' : '$pull';

        if (is_array($column)) {
            $query = [$operator => $column];
        } else {
            $query = [$operator => [$column => $value]];
        }

        return $this->performUpdate($query);
    }

    /**
     * Remove one or more fields.
     *
     * @param mixed $columns
     *
     * @return int
     */
    public function drop($columns)
    {
        if (!is_array($columns)) {
            $columns = [$columns];
        }

        $fields = [];

        foreach ($columns as $column) {
            $fields[$column] = 1;
        }

        $query = ['$unset' => $fields];

        return $this->performUpdate($query);
    }

    /**
     * Get a new instance of the query builder.
     *
     * @return Builder
     */
    public function newQuery()
    {
        return new self($this->connection, $this->processor);
    }

    /**
     * Perform an update query.
     *
     * @param array $query
     * @param array $options
     *
     * @return int
     */
    protected function performUpdate($query, array $options = [])
    {
        // Update multiple items by default.
        if (!array_key_exists('multiple', $options)) {
            $options['multiple'] = true;
        }

        $wheres = $this->compileWheres();

        $result = $this->collection->update($wheres, $query, $options);

        if (1 == (int) $result['ok']) {
            return $result['n'];
        }

        return 0;
    }

    /**
     * Convert a key to MongoID if needed.
     *
     * @param mixed $id
     *
     * @return mixed
     */
    public function convertKey($id)
    {
        if (is_string($id) and strlen($id) === 24 and ctype_xdigit($id)) {
            return new MongoId($id);
        }

        return $id;
    }

    /**
     * Add a basic where clause to the query.
     *
     * @param string $column
     * @param string $operator
     * @param mixed  $value
     * @param string $boolean
     *
     * @return \Illuminate\Database\Query\Builder|static
     *
     * @throws \InvalidArgumentException
     */
    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        $params = func_get_args();

        // Remove the leading $ from operators.
        if (func_num_args() == 3) {
            $operator = &$params[1];

            if (starts_with($operator, '$')) {
                $operator = substr($operator, 1);
            }
        }

        return call_user_func_array('parent::where', $params);
    }

    /**
     * Compile the where array.
     *
     * @return array
     */
    protected function compileWheres()
    {
        // The wheres to compile.
        $wheres = $this->wheres ?: [];

        // We will add all compiled wheres to this array.
        $compiled = [];

        foreach ($wheres as $i => &$where) {
            // Make sure the operator is in lowercase.
            if (isset($where['operator'])) {
                $where['operator'] = strtolower($where['operator']);

                // Operator conversions
                $convert = [
                    'regexp' => 'regex',
                    'elemmatch' => 'elemMatch',
                    'geointersects' => 'geoIntersects',
                    'geowithin' => 'geoWithin',
                    'nearsphere' => 'nearSphere',
                    'maxdistance' => 'maxDistance',
                    'centersphere' => 'centerSphere',
                    'uniquedocs' => 'uniqueDocs',
                ];

                if (array_key_exists($where['operator'], $convert)) {
                    $where['operator'] = $convert[$where['operator']];
                }
            }

            // Convert id's.
            if (isset($where['column']) and ($where['column'] == '_id' or ends_with($where['column'], '._id'))) {
                // Multiple values.
                if (isset($where['values'])) {
                    foreach ($where['values'] as &$value) {
                        $value = $this->convertKey($value);
                    }
                }

                // Single value.
                elseif (isset($where['value'])) {
                    $where['value'] = $this->convertKey($where['value']);
                }
            }

            // Convert DateTime values to MongoDate.
            if (isset($where['value']) and $where['value'] instanceof DateTime) {
                $where['value'] = new MongoDate($where['value']->getTimestamp());
            }

            // The next item in a "chain" of wheres devices the boolean of the
            // first item. So if we see that there are multiple wheres, we will
            // use the operator of the next where.
            if ($i == 0 and count($wheres) > 1 and $where['boolean'] == 'and') {
                $where['boolean'] = $wheres[$i + 1]['boolean'];
            }

            // We use different methods to compile different wheres.
            $method = "compileWhere{$where['type']}";
            $result = $this->{$method}($where);

            // Wrap the where with an $or operator.
            if ($where['boolean'] == 'or') {
                $result = ['$or' => [$result]];
            }

            // If there are multiple wheres, we will wrap it with $and. This is needed
            // to make nested wheres work.
            elseif (count($wheres) > 1) {
                $result = ['$and' => [$result]];
            }

            // Merge the compiled where with the others.
            $compiled = array_merge_recursive($compiled, $result);
        }

        return $compiled;
    }

    protected function compileWhereBasic($where)
    {
        extract($where);

        // Replace like with a MongoRegex instance.
        if ($operator == 'like') {
            $operator = '=';
            $regex = str_replace('%', '', $value);

            // Convert like to regular expression.
            if (!starts_with($value, '%')) {
                $regex = '^'.$regex;
            }
            if (!ends_with($value, '%')) {
                $regex = $regex.'$';
            }

            $value = new MongoRegex("/$regex/i");
        }

        // Manipulate regexp operations.
        elseif (in_array($operator, ['regexp', 'not regexp', 'regex', 'not regex'])) {
            // Automatically convert regular expression strings to MongoRegex objects.
            if (!$value instanceof MongoRegex) {
                $value = new MongoRegex($value);
            }

            // For inverse regexp operations, we can just use the $not operator
            // and pass it a MongoRegex instence.
            if (starts_with($operator, 'not')) {
                $operator = 'not';
            }
        }

        if (!isset($operator) or $operator == '=') {
            $query = [$column => $value];
        } elseif (array_key_exists($operator, $this->conversion)) {
            $query = [$column => [$this->conversion[$operator] => $value]];
        } else {
            $query = [$column => ['$'.$operator => $value]];
        }

        return $query;
    }

    protected function compileWhereNested($where)
    {
        extract($where);

        return $query->compileWheres();
    }

    protected function compileWhereIn($where)
    {
        extract($where);

        return [$column => ['$in' => array_values($values)]];
    }

    protected function compileWhereNotIn($where)
    {
        extract($where);

        return [$column => ['$nin' => array_values($values)]];
    }

    protected function compileWhereNull($where)
    {
        $where['operator'] = '=';
        $where['value'] = null;

        return $this->compileWhereBasic($where);
    }

    protected function compileWhereNotNull($where)
    {
        $where['operator'] = '!=';
        $where['value'] = null;

        return $this->compileWhereBasic($where);
    }

    protected function compileWhereBetween($where)
    {
        extract($where);

        if ($not) {
            return [
                '$or' => [
                    [
                        $column => [
                            '$lte' => $values[0],
                        ],
                    ],
                    [
                        $column => [
                            '$gte' => $values[1],
                        ],
                    ],
                ],
            ];
        } else {
            return [
                $column => [
                    '$gte' => $values[0],
                    '$lte' => $values[1],
                ],
            ];
        }
    }

    protected function compileWhereRaw($where)
    {
        return $where['sql'];
    }

    /**
     * Handle dynamic method calls into the method.
     *
     * @param string $method
     * @param array  $parameters
     *
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if ($method == 'unset') {
            return call_user_func_array([$this, 'drop'], $parameters);
        }

        return parent::__call($method, $parameters);
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     *
     * @return array|static[]
     */
    public function get($columns = ['*'])
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Execute the query and get the first result.
     *
     * @param array $columns
     *
     * @return \Illuminate\Database\Eloquent\Model|static|null
     */
    public function first($columns = [])
    {
        $repository = $this->connection->getRepository($this->documentName);

        return $repository->findOneBy($columns);
    }
}
