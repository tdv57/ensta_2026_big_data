package bigdata.mapper;

import bigdata.model.WatRecord;
import bigdata.util.CompanyRegistry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;

/**
 * Mapper — Analyse de l'élection présidentielle américaine 2024
 *
 * INPUT  : fichiers WAT Common Crawl (une ligne par ligne, mélange d'en-têtes
 *          WARC textuels et de payloads JSON)
 *
 * LOGIQUE :
 *   1. On ignore immédiatement toute ligne ne commençant pas par '{'
 *      (en-têtes WARC : "WARC/1.0", "WARC-Type: response", etc.)
 *      → réduit fortement les PARSE_ERRORS visibles dans les compteurs
 *   2. On parse le JSON → WatRecord
 *   3. On cherche si l'URL OU le titre de la page contient un mot-clé
 *      (trump, kamala, biden, election2024, …)
 *   4. Si oui → on émet la clé (candidat \t pays_TLD \t langue)
 *
 * OUTPUT : clé   = "Trump\tcom\ten"      (candidat, pays, langue)
 *          valeur= 1
 *
 * Counters utiles (visibles dans les logs après le job) :
 *   RECORDS_READ     : lignes totales lues
 *   NON_JSON_LINES   : lignes WARC textuelles ignorées rapidement (attendu : ~79%)
 *   PARSE_ERRORS     : vraies erreurs JSON (attendu : quasi 0 après correction)
 *   RECORDS_MATCHED  : pages liées à un candidat → nos données utiles
 *   RECORDS_SKIPPED  : pages parsées mais sans candidat détecté
 */
public class CrawlAnalysisMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    private final Text outputKey = new Text();
    private final CompanyRegistry registry = new CompanyRegistry();

    enum Counters {
        RECORDS_READ,
        NON_JSON_LINES,   // lignes WARC textuelles (normal)
        PARSE_ERRORS,     // vraies erreurs JSON (doit être ~0)
        RECORDS_MATCHED,
        RECORDS_SKIPPED
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new IOException(
                "Fichier de mots-clés absent du Distributed Cache ! " +
                "Utilisez -files election_keywords.csv au lancement."
            );
        }
        String localPath = new Path(cacheFiles[0].getPath()).getName();
        registry.loadFromLocalFile(localPath);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        context.getCounter(Counters.RECORDS_READ).increment(1);

        String line = value.toString();
        if (line.isEmpty()) return;

        // Filtre rapide : les en-têtes WARC ne commencent pas par '{'
        // C'était la cause des 909 970 PARSE_ERRORS dans la version précédente
        if (line.charAt(0) != '{') {
            context.getCounter(Counters.NON_JSON_LINES).increment(1);
            return;
        }

        WatRecord record = WatRecord.fromJson(line);
        if (record == null) {
            context.getCounter(Counters.PARSE_ERRORS).increment(1);
            return;
        }

        // Cherche un candidat dans l'URL + le titre de la page
        String candidate = registry.findCompany(record.getTargetUri(), record.getPageTitle());
        if (candidate == null) {
            context.getCounter(Counters.RECORDS_SKIPPED).increment(1);
            return;
        }

        context.getCounter(Counters.RECORDS_MATCHED).increment(1);

        String country  = record.getTldCountry();
        String language = record.getBestLanguage();

        // Clé de sortie : candidat \t pays \t langue
        outputKey.set(candidate + "\t" + country + "\t" + language);
        context.write(outputKey, ONE);
    }
}
