import java.io.File;
import java.io.IOException;

public class SaveStateToHdfs {

    public static String inputFileName;

    public SaveStateToHdfs(String fileName) throws IOException {
        final File inputFile = new File(fileName);
        if (!inputFile.exists()) {
            System.out.println("Cannot read input file: " + inputFile);
            return;
        }
        inputFileName = fileName;

        Process p = Runtime.getRuntime().exec(new String[]{"bash","-c","/home/liu/hadoop-3.2.0/bin/hdfs dfs -put " + inputFileName + " /state"});
        System.out.println("State file puts into HDFS.");
    }
}
