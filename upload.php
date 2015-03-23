<?php
require 'vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;
use Symfony\Component\Yaml\Parser;

define('FAILED', -1);
define('PENDING', 0);
define('PROCESSING', 1);
define('DONE', 2);


echo "Works Importer v0.2\n";
$yaml = new Parser();
$config = $yaml->parse(file_get_contents('settings.yml'));

// Load S3 connection
$s3 = S3Client::factory([
    'credentials' => [
        'key' => $config['aws']['key'],
        'secret' => $config['aws']['secret'],
    ]
]);
$s3->registerStreamWrapper();
$context = stream_context_create([
    's3' => [
        'ACL' => 'public-read'
    ]
]);

// Connect to database
try {
    $pdo = new PDO($config['database']['host'], $config['database']['username'], $config['database']['password'], $config['database']['database']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (PDOException $e) {
    echo 'Connection failed: ' . $e->getMessage();
    exit(1);
}
$hasRows = true;
try {
    while($hasRows) {
        // Load all objects to process
        $statement = $pdo->prepare("select * from _import where processed = :processed and content = 1 limit :limit;");
        $pending = PENDING;
        $statement->bindParam(':processed', $pending, PDO::PARAM_INT);
        $statement->bindParam(':limit', $config['chunk'], PDO::PARAM_INT);
        $statement->execute();

        printf("Processing %d records\n", $statement->rowCount());
        $hasRows = !!$statement->rowCount();

        // Store record objects locally
        $allFiles = $statement->fetchAll(PDO::FETCH_ASSOC);

        // Mark objects as "to be processed"
        $fileIds = array_column($allFiles, 'id');
        $placeholders = rtrim(str_repeat('?, ', count($fileIds)), ', ');
        $updateStatement = $pdo->prepare("update _import set processed = ? where id in ($placeholders);");
        $updateStatement->execute(array_merge([PROCESSING], $fileIds));


        $failedFiles = [];
        $doneFiles = [];
        foreach ($allFiles as $file) {
            try {
                // Check local hard drive for file
                $trimmedContentRoot = rtrim($config['content_root'], '/');
                $trimmedLocalFile = trim($file['local'], '/');
                $localFileLocation = sprintf('%s/%s', $trimmedContentRoot, $trimmedLocalFile);
                if (!file_exists($localFileLocation)) {
                    $failedFiles[] = $file;
                    continue;
                }

                // Check S3 for file
                $trimmedAwsFolder = trim($config['aws']['folder'], '/');
                $s3FileLocation = sprintf('s3://%s/%s/%s', $config['aws']['bucket'], $trimmedAwsFolder, $trimmedLocalFile);
                if (file_exists($s3FileLocation)) {
                    // File already exists
                    $failedFiles[] = $file; // We can check these later - mark as failed
                    continue;
                }

                // Upload file to S3
                $s3File = new SplFileObject($s3FileLocation, 'w', null, $context);
                $localFile = new SplFileObject($localFileLocation, 'r');
                while (!$localFile->eof()) {
                    $s3File->fwrite($localFile->fgets());
                }

                // Check S3 for file
                if (!file_exists($s3FileLocation)) {
                    // File already exists
                    $failedFiles[] = $file; // We can check these later - mark as failed
                    continue;
                }

                $doneFiles[] = $file;
            } catch (Exception $e) {
                $failedFiles[] = $file;
            }

        }

        // Mark objects as "processed" / "failed"
        $doneFileIds = array_column($doneFiles, 'id');
        $placeholders = rtrim(str_repeat('?, ', count($doneFileIds)), ', ');
        $updateStatement = $pdo->prepare("update _import set processed = ? where id in ($placeholders);");
        $updateStatement->execute(array_merge([DONE], $doneFileIds));
        $failedFileIds = array_column($failedFiles, 'id');
        $placeholders = rtrim(str_repeat('?, ', count($failedFileIds)), ', ');
        $updateStatement = $pdo->prepare("update _import set processed = ? where id in ($placeholders);");
        $updateStatement->execute(array_merge([FAILED], $failedFileIds));
    }
} catch (Exception $e) {
    // TODO: Handle exceptions?
}