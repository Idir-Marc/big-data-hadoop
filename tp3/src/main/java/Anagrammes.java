import org.apache.pig.PigServer;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class Anagrammes {
    public static void main(String[] args) throws IOException, URISyntaxException {
        PigServer server = new PigServer("local");
        String dataInputFile = args[0];
        String dataOutputFile = args[1];
        runner(server, dataInputFile, dataOutputFile);
    }

    private static void runner(PigServer server, String inputFile, String outputFile) throws IOException, URISyntaxException {
        server.registerJar(new File(Anagrammes.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath());
        InputStream stream = WordCount.class.getResourceAsStream("Anagrammes.pig");
        if (stream == null) {
            throw new IllegalStateException("Unable to load resource 'Anagrammes.pig'");
        }
        InputStreamReader reader = new InputStreamReader(stream);
        BufferedReader buffered = new BufferedReader(reader);
        List<String> lines = buffered.lines().collect(Collectors.toList());
        buffered.close();
        int i = 0;
        for (String s : lines) {
            server.registerQuery(s.replace("%INPUT%", inputFile).replace("%OUTPUT%", outputFile), i++);
        }
        server.executeBatch();
    }
}
