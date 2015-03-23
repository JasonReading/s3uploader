<?php
require 'vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;
use Symfony\Component\Yaml\Parser;

define('FOLDER', -2);
define('FAILED', -1);
define('PENDING', 0);
define('PROCESSING', 1);
define('DONE', 2);

echo "Works Importer v0.2\n";
$yaml = new Parser();
$config = $yaml->parse(file_get_contents('settings.yml'));

// Load S3 connection
$s3 = S3Client::factory([
    'region' => $config['aws']['region'],
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
    $pdo = new PDO($config['database']['host'], $config['database']['username'], $config['database']['password']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (PDOException $e) {
    echo 'Connection failed: ' . $e->getMessage();
    exit(1);
}
$hasRows = true;
try {
    while($hasRows) {
        // Load all objects to process
        $statement = $pdo->prepare("select * from _import where processed = :processed and content = 1 order by rand() limit :limit;");
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
        $folderFiles = [];
        foreach ($allFiles as $file) {
            try {
                // Check local hard drive for file
                $trimmedContentRoot = rtrim($config['content_root'], '/');
                $trimmedLocalFile = trim($file['local'], '/');
                $trimmedLocalFile = str_replace('./', '', $trimmedLocalFile);
                $localFileLocation = sprintf('%s/%s', $trimmedContentRoot, $trimmedLocalFile);
                $trimmedAwsFolder = trim($config['aws']['folder'], '/');
                $s3FileLocation = sprintf('%s/%s', $trimmedAwsFolder, $trimmedLocalFile);
                $s3Url = sprintf('s3://%s/%s', $config['aws']['bucket'], $s3FileLocation);
                echo str_repeat('/', 80) . "\n";
                echo $localFileLocation . "\n";
                echo $s3FileLocation . "\n";

                if (!file_exists($localFileLocation)) {
                    $failedFiles[] = $file;
                    echo " - Local file doesn't exist\n";
                    continue;
                }
                if (is_dir($localFileLocation)) {
                    $folderFiles[] = $file;
                    echo " - Local file is a folder\n";
                    continue;
                }
                echo "...";

                // Check S3 for file
                if (file_exists($s3Url)) {
                    // File already exists
                    $failedFiles[] = $file; // We can check these later - mark as failed
                    echo " - S3 file already exists\n";
                    continue;
                }
                echo "...";
                // Upload file to S3
                $s3->putObject([
                    'ACL' => 'public-read',
                    'Bucket' => $config['aws']['bucket'],
                    'Key' => $s3FileLocation,
                    'SourceFile' => $localFileLocation,
                ]);
                echo "...";

                // Check S3 file uploaded
                $s3->waitUntil('ObjectExists', [
                    'Bucket' => $config['aws']['bucket'],
                    'Key' => $s3FileLocation,
                ]);
                echo "...";

                // Check S3 for file
                if (!file_exists($s3Url)) {
                    // File already exists
                    $failedFiles[] = $file; // We can check these later - mark as failed
                    echo " - Uploaded file not found\n";
                    continue;
                }
                echo "!\n";
                $doneFiles[] = $file;
            } catch (Exception $e) {
                $failedFiles[] = $file;
            }

        }

        // Mark objects as "processed" / "failed"
        if (count($doneFiles)) {
            $doneFileIds = array_column($doneFiles, 'id');
            $placeholders = rtrim(str_repeat('?, ', count($doneFileIds)), ', ');
            $updateStatement = $pdo->prepare("update _import set processed = ? where id in ($placeholders);");
            $updateStatement->execute(array_merge([DONE], $doneFileIds));
        }
        if (count($failedFiles)) {
            $failedFileIds = array_column($failedFiles, 'id');
            $placeholders = rtrim(str_repeat('?, ', count($failedFileIds)), ', ');
            $updateStatement = $pdo->prepare("update _import set processed = ? where id in ($placeholders);");
            $updateStatement->execute(array_merge([FAILED], $failedFileIds));
        }
        if (count($folderFiles)) {
            $folderFileIds = array_column($folderFiles, 'id');
            $placeholders = rtrim(str_repeat('?, ', count($folderFileIds)), ', ');
            $updateStatement = $pdo->prepare("update _import set processed = ?, folder = ? where id in ($placeholders);");
            $updateStatement->execute(array_merge([FOLDER, 1], $failedFileIds));
        }
    }
} catch (Exception $e) {
    echo  "\n" . $e->getMessage() . "\n\n";
}