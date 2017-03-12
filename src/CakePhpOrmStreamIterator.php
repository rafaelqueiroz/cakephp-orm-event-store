<?php

namespace Prooph\EventStore\Adapter\CakePHP;

use Assert\Assertion;
use Cake\ORM\Query;
use Iterator;
use Doctrine\DBAL\Driver\PDOStatement;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\PayloadSerializer;

final class CakePhpOrmStreamIterator implements Iterator
{
    /**
     * @var Query
     */
    private $queryBuilder;
    /**
     * @var PDOStatement
     */
    private $statement;
    /**
     * @var MessageFactory
     */
    private $messageFactory;
    /**
     * @var PayloadSerializer
     */
    private $payloadSerializer;
    /**
     * @var array
     */
    private $metadata;
    /**
     * @var array
     */
    private $standardColumns = ['event_id', 'event_name', 'created_at', 'payload', 'version'];
    /**
     * @var array|false
     */
    private $currentItem;
    /**
     * @var int
     */
    private $currentKey;
    /**
     * @var int
     */
    private $batchPosition = 0;
    private $batchSize;

    /**
     * @param Query $queryBuilder
     * @param MessageFactory $messageFactory
     * @param PayloadSerializer $payloadSerializer
     * @param array $metadata
     * @param int $batchSize
     */
    public function __construct(
        Query $queryBuilder,
        MessageFactory $messageFactory,
        PayloadSerializer $payloadSerializer,
        array $metadata,
        $batchSize = 10000
    ) {
        Assertion::integer($batchSize);
        $this->queryBuilder = $queryBuilder;
        $this->messageFactory = $messageFactory;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadata = $metadata;
        $this->batchSize = $batchSize;
        $this->rewind();
    }
    /**
     * @return null|Message
     */
    public function current()
    {
        if (false === $this->currentItem) {
            return;
        }
        $payload = $this->payloadSerializer->unserializePayload(
            $this->currentItem['payload']
        );
        $metadata = [];
        //Add metadata stored in table
        foreach ($this->currentItem as $key => $value) {
            if (! in_array($key, $this->standardColumns)) {
                $metadata[$key] = $value;
            }
        }
        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $this->currentItem['created_at'],
            new \DateTimeZone('UTC')
        );
        return $this->messageFactory->createMessageFromArray($this->currentItem['event_name'], [
            'uuid' => $this->currentItem['event_id'],
            'version' => (int) $this->currentItem['version'],
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata
        ]);
    }
    /**
     * Next
     */
    public function next()
    {
        $this->currentItem = $this->stripCakePhpOrmAlias($this->statement->fetch(\PDO::FETCH_ASSOC));

        if (false !== $this->currentItem) {
            $this->currentKey++;
        } else {
            $this->batchPosition++;
            $this->queryBuilder->offset($this->batchSize * $this->batchPosition);
            $this->queryBuilder->limit($this->batchSize);
            $this->statement = $this->queryBuilder->execute();
            $this->currentItem = $this->stripCakePhpOrmAlias($this->statement->fetch(\PDO::FETCH_ASSOC));
            if (false === $this->currentItem) {
                $this->currentKey = -1;
            }
        }
    }
    /**
     * @return bool|int
     */
    public function key()
    {
        if (-1 === $this->currentKey) {
            return false;
        }
        return $this->currentKey;
    }
    /**
     * @return bool
     */
    public function valid()
    {
        return false !== $this->currentItem;
    }
    /**
     * Rewind
     */
    public function rewind()
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            $this->batchPosition = 0;
            $this->queryBuilder->offset(0);
            $this->queryBuilder->limit($this->batchSize);

            $stmt = $this->queryBuilder->execute();
            $this->currentItem = null;
            $this->currentKey = -1;
            $this->statement = $stmt;
            $this->next();
        }
    }

    private function stripCakePhpOrmAlias($item)
    {
        if (!is_array($item)) {
            return $item;
        }
        $return = [];
        foreach ($item as $key => $value) {
            if ((strpos($key, $this->queryBuilder->repository()->table() . '__') + 1) >= 1) {
                $arrKey = explode($this->queryBuilder->repository()->table() . '__', $key);
                $return[end($arrKey)] = $value;
            }
        }
        return $return;
    }
}
