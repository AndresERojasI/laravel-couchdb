<?php

namespace An3\Couchdb;

use Illuminate\Database\Eloquent\Model as BaseModel;
use Illuminate\Database\Eloquent\Relations\Relation;
use An3\Couchdb\Query\Builder as QueryBuilder;
use Illuminate\Support\Arr as Arr;
use Doctrine\ODM\CouchDB\Mapping\Annotations as ODM;

abstract class Model extends BaseModel
{
    /**
     * The primary key for the model.
     *
     * @var string
     */
    protected $primaryKey = '_id';

    /**
     *  @ODM\Field(type="string")
     */
    public $table;

    /**
     *  @ODM\Field(type="string")
     */
    public $updated_at;

    /**
     *  @ODM\Field(type="string")
     */
    public $created_at;

    /**
     *  @ODM\Field(type="string")
     */
    public $deleted_at;

    /**
     * The number of models to return for pagination.
     *
     * @var int
     */
    protected $perPage = 15;

    public $timestamps = true;

    /**
     * Indicates if the IDs are auto-incrementing.
     *
     * @var bool
     */
    public $incrementing = false;

    public $dm;

    public $dates = ['deleted_at', 'created_at', 'updated_at'];

    /**
     * Create a new Eloquent model instance.
     *
     * @param array $attributes
     */
    public function __construct(array $attributes = [])
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);

        $this->dm = $this->getConnection()->dm;
    }

    /**
     * Save a new model and return the instance.
     *
     * @param array $attributes
     *
     * @return static
     */
    public static function create(array $attributes = [])
    {
        $model = new static($attributes);

        $model->save();

        return $model;
    }

    /**
     * Save a new model and return the instance. Allow mass-assignment.
     *
     * @param array $attributes
     *
     * @return static
     */
    public static function forceCreate(array $attributes)
    {
        // Since some versions of PHP have a bug that prevents it from properly
        // binding the late static context in a closure, we will first store
        // the model in a variable, which we will then use in the closure.
        $model = new static();

        return static::unguarded(function () use ($model, $attributes) {
            return $model->create($attributes);
        });
    }

    /**
     * Get the first record matching the attributes or create it.
     *
     * @param array $attributes
     *
     * @return static
     */
    public static function firstOrCreate(array $attributes)
    {
        if (!is_null($instance = static::where($attributes)->first())) {
            return $instance;
        }

        return static::create($attributes);
    }

    /**
     * Get the first record matching the attributes or instantiate it.
     *
     * @param array $attributes
     *
     * @return static
     */
    public static function firstOrNew(array $attributes)
    {
        if (!is_null($instance = static::where($attributes)->first())) {
            return $instance;
        }

        return new static($attributes);
    }

    /**
     * Create or update a record matching the attributes, and fill it with values.
     *
     * @param array $attributes
     * @param array $values
     *
     * @return static
     */
    public static function updateOrCreate(array $attributes, array $values = [])
    {
        $instance = static::firstOrNew($attributes);

        $instance->fill($values)->save();

        return $instance;
    }

    /**
     * Begin querying the model.
     *
     * @return \Illuminate\Database\Eloquent\Builder
     */
    public static function query()
    {
        return (new static())->newQuery();
    }

    /**
     * Get all of the models from the database.
     *
     * @param array $columns
     *
     * @return \Illuminate\Database\Eloquent\Collection|static[]
     */
    public static function all($columns = ['*'])
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        $instance = new static();

        return $instance->newQuery()->get($columns);
    }

    /**
     * Find a model by its primary key or return new static.
     *
     * @param mixed $id
     * @param array $columns
     *
     * @return \Illuminate\Support\Collection|static
     */
    public static function findOrNew($id, $columns = ['*'])
    {
        if (!is_null($model = static::find($id, $columns))) {
            return $model;
        }

        return new static();
    }

    /**
     * Reload a fresh model instance from the database.
     *
     * @param array $with
     *
     * @return $this
     */
    public function fresh(array $with = [])
    {
        if (!$this->exists) {
            return;
        }

        $key = $this->getKeyName();

        return static::with($with)->where($key, $this->getKey())->first();
    }

    /**
     * Eager load relations on the model.
     *
     * @param array|string $relations
     *
     * @return $this
     */
    public function load($relations)
    {
        if (is_string($relations)) {
            $relations = func_get_args();
        }

        $query = $this->newQuery()->with($relations);

        $query->eagerLoadRelations([$this]);

        return $this;
    }

    /**
     * Begin querying a model with eager loading.
     *
     * @param array|string $relations
     *
     * @return \Illuminate\Database\Eloquent\Builder|static
     */
    public static function with($relations)
    {
        if (is_string($relations)) {
            $relations = func_get_args();
        }

        $instance = new static();

        return $instance->newQuery()->with($relations);
    }

    /**
     * Append attributes to query when building a query.
     *
     * @param array|string $attributes
     *
     * @return $this
     */
    public function append($attributes)
    {
        if (is_string($attributes)) {
            $attributes = func_get_args();
        }

        $this->appends = array_unique(
            array_merge($this->appends, $attributes)
        );

        return $this;
    }

    /**
     * Define a one-to-one relationship.
     *
     * @param string $related
     * @param string $foreignKey
     * @param string $localKey
     *
     * @return \Illuminate\Database\Eloquent\Relations\HasOne
     */
    public function hasOne($related, $foreignKey = null, $localKey = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Define an inverse one-to-one or many relationship.
     *
     * @param string $related
     * @param string $foreignKey
     * @param string $otherKey
     * @param string $relation
     *
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function belongsTo($related, $foreignKey = null, $otherKey = null, $relation = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Define a one-to-many relationship.
     *
     * @param string $related
     * @param string $foreignKey
     * @param string $localKey
     *
     * @return \Illuminate\Database\Eloquent\Relations\HasMany
     */
    public function hasMany($related, $foreignKey = null, $localKey = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Define a many-to-many relationship.
     *
     * @param string $related
     * @param string $table
     * @param string $foreignKey
     * @param string $otherKey
     * @param string $relation
     *
     * @return \Illuminate\Database\Eloquent\Relations\BelongsToMany
     */
    public function belongsToMany($related, $table = null, $foreignKey = null, $otherKey = null, $relation = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }

    /**
     * Destroy the models for the given IDs.
     *
     * @param array|int $ids
     *
     * @return int
     */
    public static function destroy($ids)
    {
        // We'll initialize a count here so we will return the total number of deletes
        // for the operation. The developers can then check this number as a boolean
        // type value or get this total count of records deleted for logging, etc.
        $count = 0;

        $ids = is_array($ids) ? $ids : func_get_args();

        $instance = new static();

        // We will actually pull the models from the database table and call delete on
        // each of them individually so that their events get fired properly with a
        // correct set of attributes in case the developers wants to check these.
        $key = $instance->getKeyName();

        foreach ($instance->whereIn($key, $ids)->get() as $model) {
            if ($model->delete()) {
                ++$count;
            }
        }

        return $count;
    }

    /**
     * Delete the model from the database.
     *
     * @return bool|null
     *
     * @throws \Exception
     */
    public function delete()
    {
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }

        if ($this->exists) {
            if ($this->fireModelEvent('deleting') === false) {
                return false;
            }

            // Here, we'll touch the owning models, verifying these timestamps get updated
            // for the models. This will allow any caching to get broken on the parents
            // by the timestamp. Then we will go ahead and delete the model instance.
            $this->touchOwners();

            $this->getConnection()->dm->remove($this);
            $this->getConnection()->dm->flush();

            $this->exists = false;

            // Once the model has been deleted, we will fire off the deleted event so that
            // the developers may hook into post-delete operations. We will then return
            // a boolean true as the delete is presumably successful on the database.
            $this->fireModelEvent('deleted', false);

            return true;
        }
    }

    /**
     * Force a hard delete on a soft deleted model.
     *
     * This method protects developers from running forceDelete when trait is missing.
     */
    public function forceDelete()
    {
        return $this->delete();
    }

    /**
     * Update the model in the database.
     *
     * @param array $attributes
     *
     * @return bool|int
     */
    public function update(array $attributes = [])
    {
        $documentName = get_class($this);

        //the model has been loaded already
        if ($this->exists) {
            $this->fill($attributes);
            $this->getConnection()->dm->flush();

            return true;
        } else {
            if (!isset($attributes['_id'])) {
                return false;
            }

            $model = $this->find($attributes['_id']);
            $model->fill($attributes);
            $model->getConnection()->dm->flush();

            return $model;
        }
    }

    /**
     * [find description].
     *
     * @return [type] [description]
     */
    public function find($id)
    {
        $documentName = get_class($this);

        if (empty($id) || empty($documentName)) {
            return false;
        }

        $model = $this->dm->find($documentName, $id);

        if (is_null($model)) {
            return false;
        }

        $attributes = get_object_vars($model);

        $model->fill($attributes);
        $model->setAttribute('_id', $attributes['_id']);
        $model->exists = true;

        return $model;
    }

    /**
     * Save the model to the database.
     *
     * @param array $options
     *
     * @return bool
     */
    public function save(array $options = [])
    {

        // If the "saving" event returns false we'll bail out of the save and return
        // false, indicating that the save failed. This provides a chance for any
        // listeners to cancel save operations if validations fail or whatever.
        if ($this->fireModelEvent('saving') === false) {
            return false;
        }

        // If the model already exists in the database we can just update our record
        // that is already in this database using the current IDs in this "where"
        // clause to only update this model. Otherwise, we'll just insert them.
        if ($this->exists) {
            $saved = $this->excecuteUpdate($options);
        }

        // If the model is brand new, we'll insert it into our database and set the
        // ID attribute on the model to the value of the newly inserted row's ID
        // which is typically an auto-increment value managed by the database.
        else {
            $saved = $this->excecuteInsert($options);
        }

        if ($saved) {
            $this->finishSave($options);
        }

        return $saved;
    }

    /**
     * [excecuteUpdate description].
     *
     * @return [type] [description]
     */
    public function excecuteUpdate(array $options = [])
    {
        if ($this->fireModelEvent('creating') === false) {
            return false;
        }

        // First we'll need to create a fresh query instance and touch the creation and
        // update timestamps on this model, which are maintained by us for developer
        // convenience. After, we will just continue saving these model instances.
        if ($this->timestamps && Arr::get($options, 'timestamps', true)) {
            $this->updateTimestamps();
        }

        // If the model has an incrementing key, we can use the "insertGetId" method on
        // the query builder, which will give us back the final inserted ID for this
        // table from the database. Not all tables have to be incrementing though.
        $attributes = $this->attributes;

        if ($this->incrementing) {
            //perform the insert and gets the ID
            $attributes['table'] = $this->table;
            $dm = $this->connection->dm;
            $result = $dm->persist($this);
            $dm->flush();
            $this->setAttribute($keyName, $id);
        }

        // If the table isn't incrementing we'll simply insert these attributes as they
        // are. These attribute arrays must contain an "id" column previously placed
        // there by the developer as the manually determined key for these models.
        else {
            $attributes['table'] = $this->table;
            $dm = $this->connection->dm;
            $result = $dm->persist($attributes);
            $dm->flush();
        }

        // We will go ahead and set the exists property to true, so that it is set when
        // the created event is fired, just in case the developer tries to update it
        // during the event. This will allow them to do so and run an update here.
        $this->exists = true;

        $this->wasRecentlyCreated = true;

        $this->fireModelEvent('created', false);

        return true;
    }

    /**
     * Perform a model insert operation.
     *
     * @param \Illuminate\Database\Eloquent\Builder $query
     * @param array                                 $options
     *
     * @return bool
     */
    protected function excecuteInsert(array $options = [])
    {
        if ($this->fireModelEvent('creating') === false) {
            return false;
        }

        // First we'll need to create a fresh query instance and touch the creation and
        // update timestamps on this model, which are maintained by us for developer
        // convenience. After, we will just continue saving these model instances.
        if ($this->timestamps && Arr::get($options, 'timestamps', true)) {
            $this->updateTimestamps();
        }

        $this->feed();

        try {
            $dm = $this->getConnection()->dm;
            $dm->persist($this);
        } catch (Exception $e) {
            return false;
        }

        $result = $dm->flush();

        $this->setAttribute('_id', $this->_id);

        // We will go ahead and set the exists property to true, so that it is set when
        // the created event is fired, just in case the developer tries to update it
        // during the event. This will allow them to do so and run an update here.
        $this->exists = true;

        $this->wasRecentlyCreated = true;

        $this->fireModelEvent('created', false);

        return true;
    }

    /**
     * [feed description].
     *
     * @return [type] [description]
     */
    public function feed()
    {
        foreach ($this->attributes as $key => $value) {
            $this->$key = $value;
        }
    }

    /**
     * Get the table qualified key name.
     *
     * @return string
     */
    public function getQualifiedKeyName()
    {
        return $this->getKeyName();
    }
    /**
     * Define an embedded one-to-many relationship.
     *
     * @param string $related
     * @param string $localKey
     * @param string $foreignKey
     * @param string $relation
     *
     * @return \An3\Couchdb\Relations\EmbedsMany
     */
    protected function embedsMany($related, $localKey = null, $foreignKey = null, $relation = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }
    /**
     * Define an embedded one-to-many relationship.
     *
     * @param string $related
     * @param string $localKey
     * @param string $foreignKey
     * @param string $relation
     *
     * @return \An3\Couchdb\Relations\EmbedsOne
     */
    protected function embedsOne($related, $localKey = null, $foreignKey = null, $relation = null)
    {
        //TODO: implement a way to do it with Doctrine
        throw new \Exception('Not implemented.');
    }
    /**
     * Convert a \Carbon\Carbon to a storable MongoDate object.
     *
     * @param \Carbon\Carbon|int $value
     *
     * @return MongoDate
     */
    public function fromDateTime($value)
    {
        // If the value is already a MongoDate instance, we don't need to parse it.
        if ($value instanceof \Carbon\Carbon) {
            return $value;
        }
        // Let Eloquent convert the value to a \Carbon\Carbon instance.
        if (!$value instanceof \Carbon\Carbon) {
            $value = parent::asDateTime($value);
        }

        return \Carbon\Carbon::now()->toDateTimeString();
    }
    /**
     * Return a timestamp as \Carbon\Carbon object.
     *
     * @param mixed $value
     *
     * @return \Carbon\Carbon
     */
    protected function asDateTime($value)
    {
        // Convert MongoDate instances.
        if ($value instanceof \Carbon\Carbon) {
            return Carbon::now();
        }

        return parent::asDateTime($value);
    }
    /**
     * Get the format for database stored dates.
     *
     * @return string
     */
    protected function getDateFormat()
    {
        return $this->dateFormat ?: 'Y-m-d H:i:s';
    }
    /**
     * Get a fresh timestamp for the model.
     *
     * @return MongoDate
     */
    public function freshTimestamp()
    {
        return \Carbon\Carbon::now()->toDateTimeString();
    }
    /**
     * Get the table associated with the model.
     *
     * @return string
     */
    public function getTable()
    {
        return $this->collection ?: parent::getTable();
    }
    /**
     * Get an attribute from the model.
     *
     * @param string $key
     *
     * @return mixed
     */
    public function getAttribute($key)
    {
        // Check if the key is an array dot notation.
        if (str_contains($key, '.') and array_has($this->attributes, $key)) {
            return $this->getAttributeValue($key);
        }
        $camelKey = camel_case($key);
        // If the "attribute" exists as a method on the model, it may be an
        // embedded model. If so, we need to return the result before it
        // is handled by the parent method.
        if (method_exists($this, $camelKey)) {
            $method = new \ReflectionMethod(get_called_class(), $camelKey);
            // Ensure the method is not static to avoid conflicting with Eloquent methods.
            if (!$method->isStatic()) {
                $relations = $this->$camelKey();
                // This attribute matches an embedsOne or embedsMany relation so we need
                // to return the relation results instead of the interal attributes.
                if ($relations instanceof EmbedsOneOrMany) {
                    // If the key already exists in the relationships array, it just means the
                    // relationship has already been loaded, so we'll just return it out of
                    // here because there is no need to query within the relations twice.
                    if (array_key_exists($key, $this->relations)) {
                        return $this->relations[$key];
                    }
                    // Get the relation results.
                    return $this->getRelationshipFromMethod($key, $camelKey);
                }
            }
        }

        return parent::getAttribute($key);
    }
    /**
     * Get an attribute from the $attributes array.
     *
     * @param string $key
     *
     * @return mixed
     */
    protected function getAttributeFromArray($key)
    {
        // Support keys in dot notation.
        if (str_contains($key, '.')) {
            $attributes = array_dot($this->attributes);
            if (array_key_exists($key, $attributes)) {
                return $attributes[$key];
            }
        }

        return parent::getAttributeFromArray($key);
    }
    /**
     * Set a given attribute on the model.
     *
     * @param string $key
     * @param mixed  $value
     */
    public function setAttribute($key, $value)
    {

        // First we will check for the presence of a mutator for the set operation
        // which simply lets the developers tweak the attribute as it is set on
        // the model, such as "json_encoding" an listing of data for storage.
        if ($this->hasSetMutator($key)) {
            $method = 'set'.studly_case($key).'Attribute';

            return $this->{$method}($value);
        }

        // If an attribute is listed as a "date", we'll convert it from a DateTime
        // instance into a form proper for storage on the database tables using
        // the connection grammar's date format. We will auto set the values.
        elseif ($value && (in_array($key, $this->getDates()) || $this->isDateCastable($key))) {
            $value = $this->fromDateTime($value);
        }

        if ($this->isJsonCastable($key) && !is_null($value)) {
            $value = json_encode($value);
        }

        $this->attributes[$key] = $value;

        return $this;
    }
    /**
     * Convert the model's attributes to an array.
     *
     * @return array
     */
    public function attributesToArray()
    {
        $attributes = parent::attributesToArray();
        // Because the original Eloquent never returns objects, we convert
        // MongoDB related objects to a string representation. This kind
        // of mimics the SQL behaviour so that dates are formatted
        // nicely when your models are converted to JSON.
        foreach ($attributes as $key => &$value) {
            if ($value instanceof MongoId) {
                $value = (string) $value;
            }
        }
        // Convert dot-notation dates.
        foreach ($this->getDates() as $key) {
            if (str_contains($key, '.') and array_has($attributes, $key)) {
                array_set($attributes, $key, (string) $this->asDateTime(array_get($attributes, $key)));
            }
        }

        return $attributes;
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
        // Unset attributes
        foreach ($columns as $column) {
            $this->__unset($column);
        }
        // Perform unset only on current document
        return $this->newQuery()->where($this->getKeyName(), $this->getKey())->unset($columns);
    }
    /**
     * Append one or more values to an array.
     *
     * @return mixed
     */
    public function push()
    {
        if ($parameters = func_get_args()) {
            $unique = false;
            if (count($parameters) == 3) {
                list($column, $values, $unique) = $parameters;
            } else {
                list($column, $values) = $parameters;
            }
            // Do batch push by default.
            if (!is_array($values)) {
                $values = [$values];
            }
            $query = $this->setKeysForSaveQuery($this->newQuery());
            $this->pushAttributeValues($column, $values, $unique);

            return $query->push($column, $values, $unique);
        }

        return parent::push();
    }
    /**
     * Remove one or more values from an array.
     *
     * @param string $column
     * @param mixed  $values
     *
     * @return mixed
     */
    public function pull($column, $values)
    {
        // Do batch pull by default.
        if (!is_array($values)) {
            $values = [$values];
        }
        $query = $this->setKeysForSaveQuery($this->newQuery());
        $this->pullAttributeValues($column, $values);

        return $query->pull($column, $values);
    }
    /**
     * Append one or more values to the underlying attribute value and sync with original.
     *
     * @param string $column
     * @param array  $values
     * @param bool   $unique
     */
    protected function pushAttributeValues($column, array $values, $unique = false)
    {
        $current = $this->getAttributeFromArray($column) ?: [];
        foreach ($values as $value) {
            // Don't add duplicate values when we only want unique values.
            if ($unique and in_array($value, $current)) {
                continue;
            }
            array_push($current, $value);
        }
        $this->attributes[$column] = $current;
        $this->syncOriginalAttribute($column);
    }
    /**
     * Remove one or more values to the underlying attribute value and sync with original.
     *
     * @param string $column
     * @param array  $values
     */
    protected function pullAttributeValues($column, array $values)
    {
        $current = $this->getAttributeFromArray($column) ?: [];
        foreach ($values as $value) {
            $keys = array_keys($current, $value);
            foreach ($keys as $key) {
                unset($current[$key]);
            }
        }
        $this->attributes[$column] = array_values($current);
        $this->syncOriginalAttribute($column);
    }
    /**
     * Set the parent relation.
     *
     * @param \Illuminate\Database\Eloquent\Relations\Relation $relation
     */
    public function setParentRelation(Relation $relation)
    {
        $this->parentRelation = $relation;
    }
    /**
     * Get the parent relation.
     *
     * @return \Illuminate\Database\Eloquent\Relations\Relation
     */
    public function getParentRelation()
    {
        return $this->parentRelation;
    }

    /**
     * Create a new Eloquent query builder for the model.
     *
     * @param \An3\Couchdb\Query\Builder $query
     *
     * @return \An3\Couchdb\Eloquent\Builder|static
     */
    public function newEloquentBuilder($query)
    {
        return new Eloquent\Builder($query);
    }

    /**
     * Get a new query builder instance for the connection.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    protected function newBaseQueryBuilder()
    {
        $conn = $this->getConnection();

        return new QueryBuilder($conn, $conn->getPostProcessor());
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
        // Unset method
        if ($method == 'unset') {
            return call_user_func_array([$this, 'drop'], $parameters);
        }

        return parent::__call($method, $parameters);
    }

    /**
     * Fill the model with an array of attributes.
     *
     * @param array $attributes
     *
     * @return $this
     *
     * @throws \Illuminate\Database\Eloquent\MassAssignmentException
     */
    public function fill(array $attributes)
    {
        $totallyGuarded = $this->totallyGuarded();

        foreach ($this->fillableFromArray($attributes) as $key => $value) {
            $key = $this->removeTableFromKey($key);

            // The developers may choose to place some attributes in the "fillable"
            // array, which means only those attributes may be set through mass
            // assignment to the model, and all others will just be ignored.
            if ($this->isFillable($key)) {
                $this->setAttribute($key, $value);
            } elseif ($totallyGuarded) {
                throw new MassAssignmentException($key);
            }
        }

        $this->feed();

        return $this;
    }

    /**
     * Get a new query builder for the model's table.
     *
     * @return \Illuminate\Database\Eloquent\Builder
     */
    public function newQuery()
    {
        $documentName = get_class($this);

        if (empty($documentName)) {
            return false;
        }

        return new QueryBuilder($this->dm, $documentName);
    }
}
