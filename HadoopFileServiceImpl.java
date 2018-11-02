import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;






@Service("hadoopFileService")
public class HadoopFileServiceImpl implements HadoopFileService {

    /**
     * Generated SerialVersionUID.
     */
    private static final long serialVersionUID = 1;

    /** The conf. */
    @Autowired
    @Qualifier(BeanConstants.HADOOP_CONF_BEAN)
    private transient Configuration conf;

    /**
     * Init method.
     */
    private void init() {

        if (conf == null) {
            conf = AnalyticsCommonService.newHadoopConf();
        }
    }

    @Override
    public void moveFromLocal(final String localPath, final String hdfsPath) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(localPath) && !StringUtils.isEmpty(hdfsPath)) {
            final Path dstPath = new Path(hdfsPath);
            final FileSystem fileSystem = dstPath.getFileSystem(conf);
            fileSystem.copyFromLocalFile(false, true, new Path(localPath), dstPath);
        }
    }

    @Override
    public void moveFromLocalAndDelete(final String localPath, final String hdfsPath) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(localPath) && !StringUtils.isEmpty(hdfsPath)) {
            final Path dstPath = new Path(hdfsPath);
            final FileSystem fileSystem = dstPath.getFileSystem(conf);
            fileSystem.moveFromLocalFile(new Path(localPath), dstPath);
        }
    }

    @Override
    public void copyFromHDFS(final String hdfsPath , final String localPath) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(localPath) && !StringUtils.isEmpty(hdfsPath)) {
            final Path srcPath = new Path(hdfsPath);
            final FileSystem fileSystem = srcPath.getFileSystem(conf);
            fileSystem.copyToLocalFile(srcPath, new Path(localPath));
        }
    }

    @Override
    public void moveFromHDFS(final String hdfsPath , final String localPath) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(localPath) && !StringUtils.isEmpty(hdfsPath)) {
            final Path srcPath = new Path(hdfsPath);
            final FileSystem fileSystem = srcPath.getFileSystem(conf);
            fileSystem.moveToLocalFile(srcPath, new Path(localPath));
        }
    }

    @Override
    public List<String> getFileNames(final String directoryPath) throws IOException {
        this.init();
        List<String> fileNames = null;
        if (!StringUtils.isEmpty(directoryPath)) {
            final Path dstPath = new Path(directoryPath);
            fileNames = new ArrayList<>();
            final FileSystem fileSystem = dstPath.getFileSystem(conf);
            final RemoteIterator<LocatedFileStatus> filesIterator = fileSystem.listLocatedStatus(dstPath);
            if (filesIterator != null) {
                while (filesIterator.hasNext()) {
                    fileNames.add(filesIterator.next().getPath().getName());
                }
            }
        }
        return fileNames;
    }

    @Override
    public List<String> getParquetFileNames(final String directoryPath) throws IOException {

        return this.getFileNames(directoryPath);
    }

    @Override
    public void move(final String source, final String destination) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(source) && !StringUtils.isEmpty(destination)) {
            final Path dstPath = new Path(destination);
            final FileSystem fileSystem = dstPath.getFileSystem(conf);
            fileSystem.rename(new Path(source), dstPath);
        }

    }

    @Override
    public void delete(final String source) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(source)) {
            final Path srcPath = new Path(source);
            final FileSystem fileSystem = srcPath.getFileSystem(conf);
            fileSystem.delete(new Path(source), true);
        }

    }

    @Override
    public void copy(final String source, final String destination) throws IOException {
        this.init();
        if (!StringUtils.isEmpty(source) && !StringUtils.isEmpty(destination)) {
            final Path dstPath = new Path(destination);
            final Path srcPath = new Path(source);
            final FileSystem dstFS = dstPath.getFileSystem(conf);
            final FileSystem srcFS = srcPath.getFileSystem(conf);
            FileUtil.copy(srcFS, srcPath, dstFS, dstPath, false, true, conf);
        }
    }

    @Override
    public InputStream getInputStream(final String source) throws IOException {
        this.init();
        InputStream is = null;
        if (!StringUtils.isEmpty(source)) {
            final Path srcPath = new Path(source);
            final FileSystem srcFS = srcPath.getFileSystem(conf);
            is = srcFS.open(srcPath);
        }
        return is;
    }
}
