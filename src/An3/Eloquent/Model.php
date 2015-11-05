<?php

namespace An3\Eloquent;

use An3\Couchdb\Eloquent\HybridRelations;

abstract class Model extends \Illuminate\Database\Eloquent\Model
{
    /*
     * Hybrid Relations
     */
    use HybridRelations;
}
