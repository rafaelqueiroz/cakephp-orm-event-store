<?php

namespace ProophTest\EventStore\Adapter\CakePHP\Mock;

use Prooph\Common\Messaging\DomainEvent;
use Prooph\Common\Messaging\PayloadConstructable;
use Prooph\Common\Messaging\PayloadTrait;

class UserWasCreated extends DomainEvent implements PayloadConstructable
{
    use PayloadTrait;

    /**
     * @param array $payload
     * @param int $version
     * @return TestDomainEvent
     */
    public static function with(array $payload, $version)
    {
        $event = new static($payload);
        return $event->withVersion($version);
    }
    /**
     * @param array $payload
     * @param int $version
     * @param \DateTimeImmutable $createdAt
     * @return TestDomainEvent
     */
    public static function withPayloadAndSpecifiedCreatedAt(array $payload, $version, \DateTimeImmutable $createdAt)
    {
        $event = new static($payload);
        $event->createdAt = $createdAt;
        return $event->withVersion($version);
    }
}