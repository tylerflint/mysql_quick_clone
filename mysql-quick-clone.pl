#!/usr/bin/perl -w

local $| = 1;

use DBI;
use DBD::mysql;
use Config::IniFiles;

my $sourceHost;
my $sourcePort;
my $sourceDatabase;
my $sourceUser;
my $sourcePass;
my $destHost;
my $destPort;
my $destDatabase;
my $destUser;
my $destPass;

my $siteRoot;
my $configFile;
my $cfg;

my @tables;
my $all = 0;
my $new = 0;

my $mysql = 'mysql';
my $mysqldump = 'mysqldump';
my $mysqlimport = 'mysqlimport';


foreach $arg (@ARGV) {
		
	if($arg =~ /^--/) {
		# If the argument begins with a hyphen, then it contains info about the database
		@argSplit = split('=', $arg);	
		
		my $value;
		
		if($value = $argSplit[1]) {			
			if($argSplit[0] eq '--sh') {				
				$sourceHost = $value;
			} elsif ($argSplit[0] eq '--sp') {
				$sourcePort = $value;
			} elsif ($argSplit[0] eq '--sd') {
				$sourceDatabase = $value;
			} elsif ($argSplit[0] eq '--su') {
				$sourceUser = $value;
			} elsif ($argSplit[0] eq '--spw') {
				$sourcePass = $value;		
			} elsif ($argSplit[0] eq '--dh') {
				$destHost = $value;
			} elsif ($argSplit[0] eq '--dp') {
				$destPort = $value;
			} elsif ($argSplit[0] eq '--dd') {
				$destDatabase = $value;
			} elsif ($argSplit[0] eq '--du') {
				$destUser = $value;
			} elsif ($argSplit[0] eq '--dpw') {
				$destPass = $value;			
			} elsif ($argSplit[0] eq '--sitedir') {
				$siteRoot = $value;
			}elsif ($argSplit[0] eq '--c') {
				$configFile = $value;
			}
		} elsif ($arg eq '--all') {
			$all = 1;
		} elsif ($arg eq '--5') {
			$mysql = 'mysql5';
			$mysqldump = 'mysqldump5';
			$mysqlimport = 'mysqlimport5'; 
		} elsif( $arg eq '--new'){
			$new = 1;
		}
		
	} else {
		push(@tables, $arg);
	}			
}

if ($configFile)
{
	$cfg = Config::IniFiles->new( -file => $configFile );
	
	$sourceHost = $cfg->val('source', 'host');
	$sourcePort = $cfg->val('source', 'port');
	$sourceDatabase = $cfg->val('source', 'database');
	$sourceUser = $cfg->val('source', 'user');
	$sourcePass = $cfg->val('source', 'password');

	$destHost = $cfg->val('destination', 'host');
	$destPort = $cfg->val('destination', 'port');
	$destDatabase = $cfg->val('destination', 'database');
	$destUser = $cfg->val('destination', 'user');
	$destPass = $cfg->val('destination', 'password');
	
	@tables = $cfg->val('tables', 'tables');
	
	$all = $cfg->val('general', 'all', 0);
	$new = $cfg->val('general', 'new', 0);
	$siteRoot = $cfg->val('general', 'siteRoot');
}

# Verify that all required arguments were provided
if(!($sourceHost && $sourcePort && $sourceDatabase && $sourceUser && $sourcePass &&
	$destHost && $destPort && $destDatabase && $destUser && $destPass && $siteRoot)) {		
	print("Please input all required arguments\n");
	exit;
}

# start timer
my $start = time();

my $sourceDsn = "dbi:mysql:$sourceDatabase:$sourceHost:$sourcePort";
my $destDsn = "dbi:mysql:$destDatabase:$destHost:$destPort";

my $sourceConn = DBI->connect($sourceDsn, $sourceUser, $sourcePass);
my $destConn = DBI->connect($destDsn, $destUser, $destPass);
$sourceConn->{mysql_auto_reconnect} = 1;
$destConn->{mysql_auto_reconnect} = 1;

if(!$sourceConn) {
	print('Could not connect to source\n\n');
	exit;
}

if(!$destConn) {
	print('Could not connect to destination\n\n');
	exit;
}

if ($all == 1)
{
	my $handle = $sourceConn->prepare("SHOW TABLES");
	$handle->execute();
	$handle->bind_columns(\$name);
	
	while($handle->fetch()) {
	   push (@tables, $name)
	} 
	 
	foreach $table (@tables) {
		print("$table ");
	}	
}

if ($new == 1)
{
	$destConn->do("SET FOREIGN_KEY_CHECKS = 0");
	$destConn->do("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'");
	foreach $table (@tables) {
		my $handle = $sourceConn->prepare("SHOW CREATE TABLE $table");
		$handle->execute();
		$handle->bind_columns(\$name, \$create_statement);
		while($handle->fetch())
		{
			$destConn->do($create_statement);
		}
	}
	$destConn->do("SET FOREIGN_KEY_CHECKS = 1  ");
}

my $errorFlag = 0;

my $backupPath = $siteRoot . "/utility/clone_backup.sql";
my $tableString = join(" ", @tables);

# Backup the tables that will be affected
print("Backing up destination tables....");
`$mysqldump --host=$destHost --user=$destUser --password=$destPass $destDatabase $tableString > $backupPath`;
print("Done\n\n");

foreach $tableName (@tables) {
	
	print("Building table $tableName....");
	my $workTable = $tableName . '_wrk';
	my $oldTable = $tableName . '_old';
	#my $filePath = "$tableName.wrk";
	my $filePath = $siteRoot . "/utility/$workTable.wrk";	
	
	# create file
	`$mysql --host=$sourceHost --user=$sourceUser --password=$sourcePass $sourceDatabase -e "SELECT * FROM $tableName" | sed 's/NULL/\\\\N/g' > $filePath`;
		
	#print("$mysql --host=$sourceHost --user=$sourceUser --password=$sourcePass $sourceDatabase -e \"SELECT * FROM $tableName\" | sed 's/NULL/\\\\N/g' > $filePath\n");
	# check if the file path exists, if it doesn't, don't do a thing
	if(-e $filePath) {
	
		# Drop table if it exists
		$destConn->do("DROP TABLE IF EXISTS $workTable");
		
		# Check table structure to see if the old and the clonee are the same
		# If so, then create table like works, if not, then we need the structure of the clonee
		# Field, Type, Null, Key, Default, Extra
		my ($newField, $newType, $newNull, $newKey, $newDefault, $newExtra);
		my ($cloneeField, $cloneeType, $cloneeNull, $cloneeKey, $cloneeDefault, $cloneeExtra);
		
		my $oldHandle = $destConn->prepare("DESC $tableName");
		$oldHandle->execute();
		$oldHandle->bind_columns(\$newField, \$newType, \$newNull, \$newKey, \$newDefault, \$newExtra);
		
		my $newHandle = $sourceConn->prepare("DESC $tableName");
		$newHandle->execute();
		$newHandle->bind_columns(\$cloneeField, \$cloneeType, \$cloneeNull, \$cloneeKey, \$cloneeDefault, \$cloneeExtra);
		
		my $tableSame = 1;
		
		while($oldHandle->fetch()&&$tableSame)
		{
			$newHandle->fetch();
			#print($newField." ".$cloneeField."\n");
			$tableSame = ($tableSame)&&($newField eq $cloneeField)&&($newType eq $cloneeType)&&($newNull eq $cloneeNull);
			if(defined($newKey))
			{
				if(defined($cloneeKey))
				{
					$tableSame = ($tableSame)&&($newKey eq $cloneeKey);
				}
				else
				{
					$tableSame = 0;
				}
			}
			else
			{
				if(defined($cloneeKey))
				{
					$tableSame = 0;
				}
			}
			if(defined($newDefault))
			{
				if(defined($cloneeDefault))
				{
					$tableSame = ($tableSame)&&($newDefault eq $cloneeDefault);
				}
				else
				{
					$tableSame = 0;
				}
			}
			else
			{
				if(defined($cloneeDefault))
				{
					$tableSame = 0;
				}
			}
			if(defined($newExtra))
			{
				if(defined($cloneeExtra))
				{
					$tableSame = ($tableSame)&&($newExtra eq $cloneeExtra);
				}
				else
				{
					$tableSame = 0;
				}
			}
			else
			{
				if(defined($cloneeExtra))
				{
					$tableSame = 0;
				}
			}             
			            
			             
		}
		while($newHandle->fetch())
		{
			$tableSame = 0;
		}
		
		if ($tableSame)
		{
			print("Tables are the same!\n");
			# Create table at destination
			$destConn->do("CREATE TABLE $workTable LIKE $tableName");
		}
		else
		{
			print("Tables are different, cloning new table structure\n");
			$sourceConn->do("CREATE TABLE $workTable LIKE $tableName");
			$handle = $sourceConn->prepare("SHOW CREATE TABLE $workTable");
			$handle->execute();
			$handle->bind_columns(\$name, \$create_statement);
			while($handle->fetch())
			{
				$destConn->do($create_statement);
			}
			$sourceConn->do("DROP TABLE IF EXISTS $workTable");
		}
	
		# Populate work table
		#$destConn->do("LOAD DATA INFILE '$filePath' INTO TABLE $workTable");
		$destConn->do("SET GLOBAL SQL_MODE='NO_AUTO_VALUE_ON_ZERO'");
		print(`$mysqlimport --local --ignore-lines=1 --host=$destHost --user=$destUser --password=$destPass $destDatabase $filePath`);
		#Now, make sure that the row count in the created table is the same as the origin table. If it isn't, don't do a thing.
#		$sourceConn->disconnect();
#		$destConn->disconnect();
#		$sourceConn = DBI->connect($sourceDsn, $sourceUser, $sourcePass);
#		$destConn = DBI->connect($destDsn, $destUser, $destPass);
		my $handle = $sourceConn->prepare("SELECT COUNT(*) FROM $tableName");	

		$handle->execute();

		my @data = $handle->fetchrow_array();
		
		my $sourceCount = 0;
		
		if(scalar @data > 0) {
			$sourceCount = $data[0];
		} else {
			# flag as error scenario
			$sourceCount = -1;
		}
		
		if($sourceCount > -1) {
			$handle = $destConn->prepare("select COUNT(*) FROM $workTable");
			
			$handle->execute();
			
			@data = $handle->fetchrow_array();
			
			my $destCount = $data[0];
			
			if(scalar @data > 0) {
				$destCount = $data[0];
				
				if($destCount == $sourceCount) {
					# Rename tables
					
					
				} else {
					#$destConn->do("DROP TABLE $workTable");
					$errorFlag = 1;
					print("Counts were different....$tableName=$sourceCount, $workTable=$destCount");
					# Remove file
					`rm $filePath`;
				}			
			} else {
				$errorFlag = 1;
				$destConn->do("DROP TABLE $workTable");
				print("Could not retrieve count from new table....");
				# Remove file
				`rm $filePath`;
			}
		} else {
			$errorFlag = 1;
			$destConn->do("DROP TABLE $workTable");
			print ("Could not retrieve count from original table....");
			# Remove file
			`rm $filePath`;
		}
	} else {
		$errorFlag = 1;
		$destConn->do("DROP TABLE $workTable");
		print("file was not created....")
	}
	
	`rm $filePath`;
	
	print("Done\n\n");
}



foreach $tableName (@tables) {

	my $oldTable = $tableName . '_old';
	my $workTable = $tableName . '_wrk';
	
	if($errorFlag == 0) {
		$destConn->do("RENAME TABLE $tableName TO $oldTable, $workTable TO $tableName");
	}
	
	# Get the table indices and drop all of them
	my @indices = &getTableConstraints($oldTable);
	
	foreach $index (@indices) 
	{
		if(defined $index && defined $oldTable) {
			$destConn->do("ALTER TABLE $oldTable DROP FOREIGN KEY '$index'");
		}
	}
}

foreach $tableName (@tables) {
	
	my $oldTable = $tableName . '_old';
	
	$destConn->do("SET FOREIGN_KEY_CHECKS = 0");	
	$destConn->do("DROP TABLE $oldTable");
	$destConn->do("SET FOREIGN_KEY_CHECKS = 1");
}

# end timer
my $end = time();

print "Total execution time: ", &formatTime($end - $start), "\n";

sub getTableConstraints
{
	my $tableName = shift;
	
	my $handle = $destConn->prepare("select CONSTRAINT_NAME from information_schema.TABLE_CONSTRAINTS where CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = '$tableName' GROUP BY CONSTRAINT_NAME");
	
	$handle->execute();
	
	my @resultArray;
	
	while (@row = $handle->fetchrow_array())
	{
		my $keyName = $row[2];
		
		push(@resultArray, $keyName);
	}
	
	return @resultArray;
}

# reset flag so that process can be used again by magento
$sourceConn->do("UPDATE core_flag SET state = 0 WHERE flag_code = 'delorum_push_to_live'");

####################################
# Get the time in a human readable format
####################################
sub formatTime{
	my $seconds = shift;
	my @parts = gmtime($seconds);
	my $time = sprintf "%d minutes, %d seconds", @parts[1,0];
	return $time;
}
