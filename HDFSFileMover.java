

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;



/**
 * File Mover implementation of HDFS.
 */
@Component(BeanConstants.HDFS_FILE_MOVER_BEAN)
public class HDFSFileMover implements FileMover {

    @Override
    public void moveFile(final String sourcePath, final String destinationDir) throws FileTransferException {

        final Configuration configuration = AnalyticsCommonService.newHadoopConf();
        try {
            final FileSystem filesystem = FileSystem.get(configuration);
            // Get hadoop destination path for file.
            final Path outPath = new Path(destinationDir);

            // Get hadoop source path for file.
            final Path inPath = new Path(sourcePath);
            // copy file from local path to hadoop destination path.
            filesystem.moveFromLocalFile(inPath, outPath);
        } catch (final IllegalArgumentException e) {
            throw new FileTransferException.FileTransferExceptionBuilder()
                .errorMessage("IllegalArgumentException occurred while moving file from local system. Filename is " + sourcePath)
                .throwable(e).build();
        } catch (final IOException e) {
            throw new FileTransferException.FileTransferExceptionBuilder()
                .errorMessage("IOException occurred while moving file from local system. Filename is " + sourcePath).throwable(e)
                .build();
        }

    }// end of method moveFile

}
