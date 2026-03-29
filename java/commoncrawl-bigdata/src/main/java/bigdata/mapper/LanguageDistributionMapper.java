package bigdata.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper du Job 3 : Répartition globale par langue
 *
 * Lit la sortie du Job 1 et émet (langue, count) pour obtenir
 * la distribution des langues sur l'ensemble du secteur analysé.
 *
 * Exemple de sortie du reduce :
 *   de    : 45 230
 *   en    : 38 100
 *   ja    : 22 800
 *   fr    : 15 300
 *   zh    : 9 200
 *   ...
 */
public class LanguageDistributionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text language = new Text();
    private final LongWritable count = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Format d'entrée : "entreprise\tpays\tlangue\tcount"
        String[] parts = line.split("\t");
        if (parts.length < 4) return;

        try {
            language.set(parts[2]);  // index 2 = langue
            count.set(Long.parseLong(parts[3]));
            context.write(language, count);
        } catch (NumberFormatException e) {
            // Ignorer
        }
    }
}
