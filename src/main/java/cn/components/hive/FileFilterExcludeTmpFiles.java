package cn.components.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileFilterExcludeTmpFiles implements PathFilter {

    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(".tmp");
    }
}
