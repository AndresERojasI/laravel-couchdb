<?php

namespace An3\Couchdb\Schema;

use Closure;
use An3\Couchdb\Connection;

class Builder extends \Illuminate\Database\Schema\Builder
{
    /**
     * Create a new database Schema manager.
     *
     * @param Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Determine if the given table has a given column.
     *
     * @param string $table
     * @param string $column
     *
     * @return bool
     */
    public function hasColumn($table, $column)
    {
        return true;
    }

    /**
     * Determine if the given table has given columns.
     *
     * @param string $table
     * @param array  $columns
     *
     * @return bool
     */
    public function hasColumns($table, array $columns)
    {
        return true;
    }
    /**
     * Determine if the given collection exists.
     *
     * @param string $collection
     *
     * @return bool
     */
    public function hasCollection($collection)
    {
        $db = $this->connection;
        $all_docs = $db->allDocs();
        if (!isset($all_docs->status) && $all_docs->status !== 200) {
            throw new Exception("Couldn't connect to the database");
        }

        $present = false;

        foreach ($all_docs->body['rows'] as $item) {
            if (isset($item['doc']['type']) && $item['doc']['type'] === $collection) {
                $present = true;
            } elseif (isset($item['type']) && $item['type'] === $collection) {
                $present = true;
            }
        }

        return $present;
    }

    /**
     * Determine if the given collection exists.
     *
     * @param string $collection
     *
     * @return bool
     */
    public function hasTable($collection)
    {
        return $this->hasCollection($collection);
    }

    /**
     * Modify a collection on the schema.
     *
     * @param string  $collection
     * @param Closure $callback
     *
     * @return bool
     */
    public function collection($collection, Closure $callback)
    {
        $blueprint = $this->createBlueprint($collection);

        if ($callback) {
            $callback($blueprint);
        }
    }

    /**
     * Modify a collection on the schema.
     *
     * @param string  $collection
     * @param Closure $callback
     *
     * @return bool
     */
    public function table($collection, Closure $callback)
    {
        return $this->collection($collection, $callback);
    }

    /**
     * Create a new collection on the schema.
     *
     * @param string  $collection
     * @param Closure $callback
     *
     * @return bool
     */
    public function create($collection, Closure $callback = null)
    {
        $blueprint = $this->createBlueprint($collection);

        $blueprint->create();
    }

    /**
     * Drop a collection from the schema.
     *
     * @param string $collection
     *
     * @return bool
     */
    public function drop($collection)
    {
        $blueprint = $this->createBlueprint($collection);

        return $blueprint->drop();
    }

    /**
     * Create a new Blueprint.
     *
     * @param string $collection
     *
     * @return Schema\Blueprint
     */
    protected function createBlueprint($collection, Closure $callback = null)
    {
        return new Blueprint($this->connection, $collection);
    }
}
