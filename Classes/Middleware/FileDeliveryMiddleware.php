<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Middleware;

use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use TYPO3\CMS\Core\Resource\FileInterface;
use TYPO3\CMS\Core\Resource\ResourceFactory;

class FileDeliveryMiddleware implements MiddlewareInterface
{
    /**
     * @var ResponseFactoryInterface
     */
    private $responseFactory;

    /**
     * @var ResourceFactory
     */
    private $resourceFactory;

    /**
     * FileDeliveryMiddleware constructor.
     * @param ResponseFactoryInterface $responseFactory
     * @param ResourceFactory $resourceFactory
     */
    public function __construct(ResponseFactoryInterface $responseFactory, ResourceFactory $resourceFactory)
    {
        $this->responseFactory = $responseFactory;
        $this->resourceFactory = $resourceFactory;
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        if (!$this->isResponsible($request)) {
            return $handler->handle($request);
        }

        $fileIdentifier = $request->getQueryParams()['id'] ?? null;
        if (null === $fileIdentifier || !\is_string($fileIdentifier) || '' === $fileIdentifier) {
            return $this->responseFactory->createResponse(404, 'Not Found');
        }

        $file = $this->resourceFactory->getFileObjectFromCombinedIdentifier($fileIdentifier);
        if (!($file instanceof FileInterface)) {
            return $this->responseFactory->createResponse(404, 'Not Found');
        }

        $response = $this->responseFactory
            ->createResponse(200, 'Ok')
            ->withHeader('Content-Type', $file->getMimeType())
            ->withHeader('Content-Disposition', 'inline; filename="' . $file->getName() . '"');
        $response->getBody()->write($file->getContents());

        return $response;
    }

    private function isResponsible(ServerRequestInterface $request): bool
    {
        $eidParameterMatches = 'fal_database_download' === ($request->getQueryParams()['eID'] ?? '');
        $fileIdParameterPresent = \array_key_exists('id', $request->getQueryParams());
        return 'GET' === \mb_strtoupper($request->getMethod())
            && $eidParameterMatches
            && $fileIdParameterPresent;
    }
}
