package fileio;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Java8FileIO {

    public static void main(final String...args){

        final Java8FileIO jva8FileIO = new Java8FileIO();

        try{
            //jva8FileIO.readCompleteFile("E:/lines.txt");

            jva8FileIO.readFileLineWise("E:/lines.txt");
        }catch(final IOException e){
            e.printStackTrace();
        }catch(final Exception e){
            e.printStackTrace();
        }

    }

    public void readCompleteFile(final String fileName) throws IOException{
        System.out.println("-- inside readCompleteFile --");
        final List<String> allRows = Files.readAllLines(Paths.get(fileName));

        System.out.println("-- read file --");

        final List<String> allRows2 = allRows.stream().map((final String row) -> {
            System.out.println(row);
            return row.trim();
        }).collect(Collectors.toList());

    }

    
    public void readFileLineWise(final String fileName) throws IOException{
        System.out.println("-- method entry readFileLineWise --");
        Files.lines(new File(fileName).toPath())
        .map(s -> s.trim())
        .filter(s -> !s.isEmpty())
        .forEach(System.out::println);

        System.out.println("-- method exit readFileLineWise --");
    }

    
    public void readWriteFileLineWise(final String inputFileName, final String outputFileName) throws IOException{
        System.out.println("-- method entry readWriteFileLineWise --");
        final File outputFIle = new File(outputFileName);

        Files.lines(new File(inputFileName).toPath())
        .map(s -> s.trim())
        .filter(s -> !s.isEmpty())
        .forEach((row) - > {
            Files.write(outputFIle, row.getBytes(), StandardOpenOption.CREATE);
        });
        

        System.out.println("-- method exit readFileLineWise --");
    }

}
