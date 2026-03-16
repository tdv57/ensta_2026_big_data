package bigdata.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer principal — Job 1 : Agrégation des comptages
 *
 * INPUT  : clé   = "entreprise\tpays\tlangue"  (Text)
 *          valeurs = liste de 1                  (LongWritable)
 *
 * OUTPUT : clé   = "entreprise\tpays\tlangue"  (Text)
 *          valeur= total pages                  (LongWritable)
 *
 * Le fichier de sortie peut être lu comme un TSV :
 *   Volkswagen    de    de    14230
 *   Volkswagen    com   en    8921
 *   Toyota        jp    ja    21034
 *   ...
 *
 * Un Combiner identique (LongSumReducer ou cette même classe) peut être
 * utilisé pour réduire le trafic réseau entre Mappers et Reducers.
 */
public class CrawlAnalysisReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        result.set(sum);
        context.write(key, result);
    }
}
