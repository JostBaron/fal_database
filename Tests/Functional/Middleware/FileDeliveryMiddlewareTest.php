<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Middleware;

use Jbaron\FalDatabase\Middleware\FileDeliveryMiddleware;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\RequestHandlerInterface;
use TYPO3\CMS\Core\Core\Environment;
use TYPO3\CMS\Core\Http\Response;
use TYPO3\CMS\Core\Http\ServerRequest;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class FileDeliveryMiddlewareTest extends AbstractFunctionalTestCase
{
    /**
     * Status code of the response of the next handler, to detect pass-through.
     */
    private const PASSED_THROUGH_STATUS = 418;

    protected function setUp(): void
    {
        parent::setUp();
        $storage = $this->getStorage(self::DATABASE_STORAGE_UID);
        $storage->addFile(
            $this->createLocalFile(self::pngContents(), 'png'),
            $storage->getRootLevelFolder(),
            'image.png'
        );
    }

    /**
     * @test
     */
    public function middlewareIsRegisteredInFrontendStackBeforeEidHandling(): void
    {
        $identifiers = array_keys(GeneralUtility::getContainer()->get('frontend.middlewares')->getArrayCopy());

        $ownPosition = array_search('jbaron/fal_database/filedelivery', $identifiers, true);
        $eidPosition = array_search('typo3/cms-frontend/eid', $identifiers, true);
        $normalizedParamsPosition = array_search('typo3/cms-core/normalized-params-attribute', $identifiers, true);

        self::assertIsInt($ownPosition, 'Middleware is not registered.');
        self::assertIsInt($eidPosition);
        self::assertIsInt($normalizedParamsPosition);
        // MiddlewareDispatcher wraps each middleware around the previous ones, so the
        // resolved list is in reverse execution order: a higher index runs earlier.
        self::assertGreaterThan($eidPosition, $ownPosition, 'Must run before eID handling.');
        self::assertLessThan($normalizedParamsPosition, $ownPosition, 'Must run after normalized params are set.');
    }

    /**
     * @test
     */
    public function deliversFileFromDatabaseStorage(): void
    {
        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '1:/image.png']);

        self::assertSame(200, $response->getStatusCode());
        self::assertSame('image/png', $response->getHeaderLine('Content-Type'));
        self::assertSame('inline; filename="image.png"', $response->getHeaderLine('Content-Disposition'));
        self::assertSame(self::pngContents(), (string)$response->getBody());
    }

    /**
     * @test
     */
    public function urlFromPublicUrlIsDelivered(): void
    {
        $publicUrl = (string)$this->getStorage(self::DATABASE_STORAGE_UID)->getFile('/image.png')->getPublicUrl();
        parse_str((string)parse_url($publicUrl, PHP_URL_QUERY), $queryParameters);

        $response = $this->process('GET', $queryParameters);

        self::assertSame(200, $response->getStatusCode());
        self::assertSame(self::pngContents(), (string)$response->getBody());
    }

    /**
     * @return array<string, array{0: string, 1: array<string, string>}>
     */
    public function notResponsibleRequestsDataProvider(): array
    {
        return [
            'no eID' => ['GET', ['id' => '1:/image.png']],
            'other eID' => ['GET', ['eID' => 'something_else', 'id' => '1:/image.png']],
            'no id' => ['GET', ['eID' => 'fal_database_download']],
            'POST' => ['POST', ['eID' => 'fal_database_download', 'id' => '1:/image.png']],
        ];
    }

    /**
     * @test
     * @dataProvider notResponsibleRequestsDataProvider
     *
     * @param array<string, string> $queryParameters
     */
    public function passesOtherRequestsOn(string $method, array $queryParameters): void
    {
        self::assertSame(self::PASSED_THROUGH_STATUS, $this->process($method, $queryParameters)->getStatusCode());
    }

    /**
     * @test
     */
    public function returns404ForEmptyId(): void
    {
        self::assertSame(404, $this->process('GET', ['eID' => 'fal_database_download', 'id' => ''])->getStatusCode());
    }

    /**
     * @test
     */
    public function returns404ForMissingFile(): void
    {
        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '1:/missing.png']);

        self::assertSame(404, $response->getStatusCode());
    }

    /**
     * @test
     */
    public function returns404ForMissingStorage(): void
    {
        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '999:/image.png']);

        self::assertSame(404, $response->getStatusCode());
    }

    /**
     * Files of other storages have their own delivery and access rules (e.g. a
     * non-public local storage); this endpoint must not bypass them.
     *
     * @test
     */
    public function doesNotDeliverFilesFromStoragesWithOtherDrivers(): void
    {
        $path = Environment::getPublicPath() . '/local-storage/';
        $this->createLocalStorage(self::LOCAL_STORAGE_UID, $path);
        file_put_contents($path . 'secret.txt', 'secret');

        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '2:/secret.txt']);

        self::assertSame(404, $response->getStatusCode());
        self::assertStringNotContainsString('secret', (string)$response->getBody());
    }

    /**
     * Storage 0 is TYPO3's fallback storage, which maps the whole public directory.
     * TYPO3 itself refuses access to PHP files there; the endpoint must turn that into
     * a 404 instead of an unhandled exception.
     *
     * @test
     */
    public function doesNotDeliverArbitraryFilesThroughTheFallbackStorage(): void
    {
        $configurationFile = 'typo3conf/LocalConfiguration.php';
        self::assertFileExists(Environment::getPublicPath() . '/' . $configurationFile);

        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '0:/' . $configurationFile]);

        self::assertSame(404, $response->getStatusCode());
        self::assertStringNotContainsString('TYPO3_CONF_VARS', (string)$response->getBody());
        self::assertStringNotContainsString("'DB'", (string)$response->getBody());
    }

    /**
     * @test
     */
    public function contentDispositionFilenameIsEscaped(): void
    {
        $storage = $this->getStorage(self::DATABASE_STORAGE_UID);
        $storage->addFile($this->createLocalFile('x'), $storage->getRootLevelFolder(), 'quote".txt');

        $response = $this->process('GET', ['eID' => 'fal_database_download', 'id' => '1:/quote".txt']);

        self::assertSame(200, $response->getStatusCode());
        self::assertMatchesRegularExpression(
            '/^inline; filename="(?:[^"\\\\]|\\\\.)*"(?:;.*)?$/',
            $response->getHeaderLine('Content-Disposition'),
            'The filename must be a valid quoted string.'
        );
    }

    /**
     * @param array<string, string> $queryParameters
     */
    private function process(string $method, array $queryParameters): ResponseInterface
    {
        $request = (new ServerRequest('https://example.com/index.php?' . http_build_query($queryParameters), $method))
            ->withQueryParams($queryParameters);

        $nextHandler = new class () implements RequestHandlerInterface {
            public function handle(ServerRequestInterface $request): ResponseInterface
            {
                return new Response('php://temp', 418);
            }
        };

        return (new FileDeliveryMiddleware())->process($request, $nextHandler);
    }
}
