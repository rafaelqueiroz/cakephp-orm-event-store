<?php

namespace Prooph\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use Cake\Datasource\ConnectionInterface;
use Cake\Database\Schema\TableSchema;
use Cake\ORM\Table;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStore\Stream\Stream;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Iterator;
use DateTimeInterface;

class CakePhpOrmEventStoreAdapter implements Adapter
{
    /**
     * Custom sourceType to table mapping
     *
     * @var array
     */
    private $streamTableMap = [];
    /**
     * @var Connection $connection
     */
    private $connection;
    /**
     * @var FQCNMessageFactory $messageFactory
     */
    private $messageFactory;
    /**
     * @var NoOpMessageConverter $messageConverter
     */
    private $messageConverter;
    /**
     * @var JsonPayloadSerializer $payloadSerializer
     */
    private $payloadSerializer;

    /**
     * CakePhpOrmEventStoreAdapter constructor.
     *
     * @param Connection $connection
     * @param FQCNMessageFactory $messageFactory
     * @param NoOpMessageConverter $messageConverter
     * @param JsonPayloadSerializer $payloadSerializer
     */
    public function __construct(
        Connection $connection,
        FQCNMessageFactory $messageFactory,
        NoOpMessageConverter $messageConverter,
        JsonPayloadSerializer $payloadSerializer
    ) {
        $this->connection = $connection;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
    }

    /**
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param bool $returnSql
     * @return array|void If $returnSql is set to true then method returns array of SQL strings
     */
    public function createSchemaFor(StreamName $streamName, array $metadata, $returnSql = false)
    {
        $tableSchema = new TableSchema($this->getTable($streamName));
        static::addToTableSchema($tableSchema, $metadata);
        $sqls = $tableSchema->createSql($this->connection);
        if ($returnSql) {
            return $sqls;
        }
        foreach ($sqls as $sql) {
            $this->connection->execute($sql);
        }
    }

    /**
     * @param Schema $schema
     * @param string $table
     * @param array $metadata
     */
    public static function addToTableSchema(TableSchema $tableSchema, array $metadata)
    {
        $tableSchema->addColumn('event_id', ['type' => 'string', 'length' => 36]);
        $tableSchema->addColumn('version', ['type' => 'integer']);
        $tableSchema->addColumn('event_name', ['type' => 'string', 'length' => 100]);
        $tableSchema->addColumn('payload', ['type' => 'text']);
        $tableSchema->addColumn('created_at', ['type' => 'string', 'length' => 50]);
        foreach ($metadata as $key => $value) {
            $tableSchema->addColumn($key,['type' => 'string', 'length' => 100]);
        }
        if ($tableSchema->column('aggregate_id')) {
            $tableSchema->addConstraint('aggregate_id', ['columns' => 'aggregate_id', 'type' => 'unique']);
            $tableSchema->addConstraint('version', ['columns' => 'version', 'type' => 'unique']);
        }
        $tableSchema->addConstraint('event_id',['columns' => 'event_id','type' => 'primary']);
    }


    /**
     * Get table name for given stream name
     *
     * @param StreamName $streamName
     * @return string
     */
    public function getTable(StreamName $streamName)
    {
        if (isset($this->streamTableMap[$streamName->toString()])) {
            $tableName = $this->streamTableMap[$streamName->toString()];
        } else {
            $tableName = strtolower($this->getShortStreamName($streamName));
            if (strpos($tableName, "_stream") === false) {
                $tableName.= "_stream";
            }
        }
        return $tableName;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }

    public function load(StreamName $streamName, $minVersion = null)
    {
        // TODO: Implement load() method.
    }

    public function create(Stream $stream)
    {
        // TODO: Implement create() method.
    }

    public function appendTo(StreamName $streamName, Iterator $domainEvents)
    {
        // TODO: Implement appendTo() method.
    }

    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        // TODO: Implement replay() method.
    }

    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        // TODO: Implement loadEvents() method.
    }
}