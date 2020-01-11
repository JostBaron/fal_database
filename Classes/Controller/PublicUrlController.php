<?php

declare(strict_types=1);

/*
 * Copyright (C) 2020 mein Bauernhof GbR.
 *
 * This file is subject to the terms and conditions defined in the
 * file 'LICENSE.txt', which is part of this source code package.
 */

namespace Jbaron\FalDatabase\Controller;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use TYPO3\CMS\Core\Resource\ResourceFactory;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class PublicUrlController
{
    private const PARAMETER_FILE_IDENTIFIER = 'id';

    /**
     * @var ResourceFactory
     */
    protected $resourceFactory;

    public function __construct()
    {
        $this->resourceFactory = ResourceFactory::getInstance();
    }

    public function dumpFile(RequestInterface $request, ResponseInterface $response): ResponseInterface
    {
        $queryString = $request->getUri()->getQuery();
        \parse_str($queryString, $parsedQuery);

        if (!\array_key_exists(self::PARAMETER_FILE_IDENTIFIER, $parsedQuery)) {
            return $response->withStatus(404);
        }

        $combinedFileIdentifier = $parsedQuery[self::PARAMETER_FILE_IDENTIFIER];

        $file = $this->resourceFactory->getFileObjectFromCombinedIdentifier($combinedFileIdentifier);

        $response = $response
            ->withStatus(200)
            ->withHeader('Content-Type', $file->getMimeType())
            ->withHeader('Content-Disposition', 'inline; filename="' . $file->getName() . '"');
        $response->getBody()->write($file->getContents());

        return $response;
    }
}
