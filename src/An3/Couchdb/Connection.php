<?php

namespace An3\Couchdb;

use Doctrine\Common\Annotations\AnnotationReader as AnnotationReader;
use Doctrine\ODM\CouchDB\Mapping\Driver\AnnotationDriver as AnnotationDriver;
use Doctrine\Common\Annotations\AnnotationRegistry as AnnotationRegistry;
use Doctrine\CouchDB\View\FolderDesignDocument;

class Connection extends \Illuminate\Database\Connection
{
    protected $db;
    public $dm;
    protected $connection;
    protected $config;
    protected $configManager;
    protected $metadataDriver;

    /**
     * Create a new database connection instance.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        //now we change the default values for the ones the user configured
        $default_config = [
            'database' => '',
            'host' => 'localhost',
            'port' => 5984,
            'username' => null,
            'password' => null,
            'ip' => null ,
            'ssl' => false,
            'models_dir' => app_path(),
            'lucene_handler_name' => '_fti',
            'proxies_dir' => app_path().'storage'.DIRECTORY_SEPARATOR.'proxies',
            'keep-alive' => true,
            'timeout' => '0.01',
            'views_folder' => '../app/couchdb',
        ];

        $config = array_replace_recursive($default_config, $config);

        //let's obtain the configuration parameters passed by configuration
        $this->config = $config;

        $this->setQueryGrammar(new \Illuminate\Database\Query\Grammars\Grammar());

        try {
            // Create the connection
            $this->connection = $this->createConnection();
            $this->db = $this->connection;
            $this->connection->getDatabaseInfo();
        } catch (\Doctrine\CouchDB\HTTP\HTTPException $e) {
            $this->connection->createDatabase($this->connection->getDatabase());
        }

        $this->useDefaultPostProcessor();
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor();
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param string $collection
     *
     * @return QueryBuilder
     */
    public function collection($collection)
    {
        $processor = $this->getPostProcessor();

        $query = new Query\Builder($this, $processor);

        return $query->from($collection);
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param string $table
     *
     * @return QueryBuilder
     */
    public function table($table)
    {
        return $this->collection($table);
    }

    /**
     * Get a CouchDB collection.
     *
     * @param string $name
     *
     * @return CouchDB
     */
    public function getCollection($name)
    {
        try {
            $document = $this->dm->getRepository($name);

            return $document;
        } catch (\Doctrine\ODM\CouchDB\Mapping\MappingException $e) {
            $exception = new \Illuminate\Database\Eloquent\ModelNotFoundException();
            $exception->setModel($name);
            throw $exception;
        }
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return Schema\Builder
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * Get the CouchDB database object.
     *
     * @return CouchDB
     */
    public function getCouchDB()
    {
        return $this->db;
    }

    /**
     * return MongoClient object.
     *
     * @return MongoClient
     */
    public function getCouchClient()
    {
        return $this->connection;
    }

    /**
     * Create a new MongoClient connection.
     *
     * @param string $dsn
     * @param array  $config
     * @param array  $options
     *
     * @return MongoClient
     */
    protected function createConnection()
    {
        $databaseName = $this->config['database'];
        $documentPaths = array($this->config['models_dir']);
        $httpClient = new \Doctrine\CouchDB\HTTP\SocketClient($this->config['host'], $this->config['port'], $this->config['username'], $this->config['password'], $this->config['ip'], $this->config['ssl']);
        $httpClient->setOption('keep-alive', $this->config['keep-alive']);

        //$httpClient->setOption('timeout', $this->config['timeout']);
        $this->configManager = new \Doctrine\ODM\CouchDB\Configuration();

        //$this->metadataDriver = $this->configManager->newDefaultAnnotationDriver($documentPaths);
        $this->metadataDriver = new AnnotationDriver(new AnnotationReader(), $documentPaths);

        // registering noop annotation autoloader - allow all annotations by default
        AnnotationRegistry::registerLoader('class_exists');

        $this->configManager->setProxyDir($this->config['proxies_dir']);
        $this->configManager->setMetadataDriverImpl($this->metadataDriver);
        $this->configManager->setLuceneHandlerName($this->config['lucene_handler_name']);

        $connection = new \Doctrine\CouchDB\CouchDBClient($httpClient, $databaseName);
        $this->dm = new \Doctrine\ODM\CouchDB\DocumentManager($connection, $this->configManager);

        $view = new FolderDesignDocument($this->config['views_folder']);

        $connection->createDesignDocument('couchdb', $view);

        return $connection;
    }

    /**
     * Disconnect from the underlying MongoClient connection.
     */
    public function disconnect()
    {
        //TODO: does Couchdb closes?
    }

    /**
     * Get the elapsed time since a given starting point.
     *
     * @param int $start
     *
     * @return float
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
     * Get the PDO driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return 'couchdb';
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param string $method
     * @param array  $parameters
     *
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array([$this->db, $method], $parameters);
    }

    /**
     * Run a select statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     *
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = false)
    {
    }
}
