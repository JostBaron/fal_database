<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Middleware;

use InvalidArgumentException;
use Jbaron\FalDatabase\Driver\DatabaseDriver;
use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;
use TYPO3\CMS\Core\Resource\Exception as ResourceException;
use TYPO3\CMS\Core\Resource\FileInterface;
use TYPO3\CMS\Core\Resource\ResourceStorage;
use TYPO3\CMS\Core\Resource\StorageRepository;
use TYPO3\CMS\Core\Utility\GeneralUtility;

/**
 * Delivers files of database storages: the target of DatabaseDriver::getPublicUrl().
 *
 * Only files of online, public storages using the database driver are delivered.
 * Everything else gets a 404, so the endpoint can't be used to bypass the delivery
 * and access rules of other storages.
 */
class FileDeliveryMiddleware implements MiddlewareInterface
{
    private ResponseFactoryInterface $responseFactory;

    private StorageRepository $storageRepository;

    public function __construct()
    {
        $this->responseFactory = GeneralUtility::makeInstance(ResponseFactoryInterface::class);
        $this->storageRepository = GeneralUtility::makeInstance(StorageRepository::class);
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        if (!$this->isResponsible($request)) {
            return $handler->handle($request);
        }

        $file = $this->findDeliverableFile($request->getQueryParams()['id']);
        if (null === $file) {
            return $this->responseFactory->createResponse(404, 'Not Found');
        }

        $response = $this->responseFactory
            ->createResponse(200, 'OK')
            ->withHeader('Content-Type', $file->getMimeType())
            ->withHeader('Content-Disposition', $this->getContentDisposition($file->getName()));
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

    /**
     * @param mixed $combinedIdentifier The "id" parameter, "<storage uid>:<file identifier>".
     */
    private function findDeliverableFile($combinedIdentifier): ?FileInterface
    {
        if (!\is_string($combinedIdentifier) || 1 !== \preg_match('/^(\d+):(\/.+)$/s', $combinedIdentifier, $matches)) {
            return null;
        }
        $storageUid = (int)$matches[1];
        $fileIdentifier = $matches[2];

        // Check the storage before resolving the file: resolving creates index entries.
        $storage = $storageUid > 0 ? $this->storageRepository->findByUid($storageUid) : null;
        if (!$storage instanceof ResourceStorage
            || DatabaseDriver::DRIVER_KEY !== $storage->getDriverType()
            || !$storage->isOnline()
            || !$storage->isPublic()
        ) {
            return null;
        }

        try {
            if (!$storage->hasFile($fileIdentifier)) {
                return null;
            }
            $file = $storage->getFile($fileIdentifier);
        } catch (ResourceException | InvalidArgumentException $exception) {
            return null;
        }

        return $file instanceof FileInterface ? $file : null;
    }

    /**
     * Builds an inline Content-Disposition header (RFC 6266): an escaped ASCII
     * fallback filename, plus the exact UTF-8 filename for clients supporting it.
     */
    private function getContentDisposition(string $fileName): string
    {
        $asciiFallback = (string)\preg_replace('/[^\x20-\x7e]/', '_', $fileName);
        $asciiFallback = \addcslashes($asciiFallback, '"\\');

        return \sprintf(
            'inline; filename="%s"; filename*=UTF-8\'\'%s',
            $asciiFallback,
            \rawurlencode($fileName)
        );
    }
}
