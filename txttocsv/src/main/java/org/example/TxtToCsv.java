package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

public class TxtToCsv{

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: TxtToCsvConverter <input_path> <output_path>");
            return;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = new Configuration();

        try {
            FileSystem fs = FileSystem.get(configuration);
            Path inFile = new Path(inputPath);
            Path outFile = new Path(outputPath);

            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                return;
            }

            if (fs.exists(outFile)) {
                System.out.println("Output file already exists");
                return;
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outFile, true)))) {

                String line;
                boolean firstLine = true;

                while ((line = br.readLine()) != null) {
                    // Skip the separator line containing dashes
                    if (line.contains("----------")) {
                        continue;
                    }

                    if (firstLine) { // Write header to the CSV
                        firstLine = false;
                        bw.write("SEANCE;GROUPE;CODE;VALEUR;OUVERTURE;CLOTURE;PLUS_BAS;PLUS_HAUT;QUANTITE_NEGOCIEE;NB_TRANSACTION;CAPITAUX");
                        bw.newLine();
                        continue;
                    }

                    // Using substring to handle fixed-width formatted lines
                    try {
                        String seance = line.substring(0, 10).trim();
                        String groupe = line.substring(10, 17).trim();
                        String code = line.substring(17, 30).trim();
                        String valeur = line.substring(30, 48).trim();
                        String ouverture = line.substring(48, 60).trim();
                        String cloture = line.substring(60, 72).trim();
                        String plusBas = line.substring(72, 84).trim();
                        String plusHaut = line.substring(84, 96).trim();
                        String quantiteNegociee = line.substring(96, 111).trim();
                        String nbTransaction = line.substring(111, 127).trim();
                        String capitaux = line.substring(127).trim();

                        String csvLine = String.join(";", new String[] {seance, groupe, code, valeur, ouverture, cloture, plusBas, plusHaut, quantiteNegociee, nbTransaction, capitaux});
                        bw.write(csvLine);
                        bw.newLine();
                    } catch (StringIndexOutOfBoundsException e) {
                        System.out.println("Error processing line: " + line);
                        continue;
                    }
                }
            }
            System.out.println("File conversion completed.");
        } catch (Exception e) {
            System.out.println("Error during file processing: " + e.getMessage());
        }
    }
}
