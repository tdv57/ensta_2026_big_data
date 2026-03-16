package bigdata.job;

import bigdata.mapper.CrawlAnalysisMapper;
import bigdata.mapper.CompanyTotalMapper;
import bigdata.mapper.LanguageDistributionMapper;
import bigdata.reducer.CrawlAnalysisReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * ============================================================
 *  DRIVER PRINCIPAL — CommonCrawl Automotive Analysis
 * ============================================================
 *
 * Lance 3 jobs MapReduce en séquence :
 *
 *  Job 1 : CrawlAnalysis
 *    - Input  : fichiers WAT Common Crawl (HDFS)
 *    - Output : entreprise \t pays_tld \t langue \t count
 *    - But    : compter les pages par (entreprise, pays, langue)
 *
 *  Job 2 : CompanyTotal
 *    - Input  : sortie Job 1
 *    - Output : entreprise \t total_pages
 *    - But    : classement global des entreprises
 *
 *  Job 3 : LanguageDistribution
 *    - Input  : sortie Job 1
 *    - Output : langue \t total_pages
 *    - But    : répartition globale des langues dans le secteur
 *
 * ============================================================
 *  LANCEMENT
 * ============================================================
 *
 *  # Copier les fichiers WAT sur HDFS
 *  hdfs dfs -mkdir -p /user/hadoop/crawl/input
 *  hdfs dfs -put CC-MAIN-*.warc.wat.gz /user/hadoop/crawl/input/
 *
 *  # Lancer le job
 *  hadoop jar commoncrawl-analysis-1.0-SNAPSHOT.jar bigdata.job.CrawlAnalysisDriver \
 *      -files election_keywords.csv \
 *      /user/hadoop/crawl/input \
 *      /user/hadoop/crawl/output
 *
 *  # Lire les résultats
 *  hdfs dfs -cat /user/hadoop/crawl/output/job1/part-r-*   # détail
 *  hdfs dfs -cat /user/hadoop/crawl/output/job2/part-r-*   # totaux entreprises
 *  hdfs dfs -cat /user/hadoop/crawl/output/job3/part-r-*   # langues
 */
public class CrawlAnalysisDriver extends Configured implements Tool {

    private static final String COMPANIES_FILE = "election_keywords.csv";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CrawlAnalysisDriver <inputPath> <outputPath>");
            System.err.println("  inputPath  : répertoire HDFS contenant les fichiers WAT");
            System.err.println("  outputPath : répertoire HDFS de sortie (ne doit pas exister)");
            return 1;
        }

        String inputPath  = args[0];
        String outputBase = args[1];

        // Chemins des sorties intermédiaires et finales
        String job1Output = outputBase + "/job1_detail";
        String job2Output = outputBase + "/job2_company_totals";
        String job3Output = outputBase + "/job3_language_dist";

        // =====================================================================
        // JOB 1 : Analyse principale
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println(" JOB 1 : Analyse WAT Common Crawl");
        System.out.println("========================================");

        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1, "CommonCrawl - Election US 2024 : Trump vs Harris vs Biden");
        job1.setJarByClass(CrawlAnalysisDriver.class);

        // Distributed Cache : fichier CSV des entreprises
        job1.addCacheFile(new URI(COMPANIES_FILE + "#" + COMPANIES_FILE));

        // Mapper / Combiner / Reducer
        job1.setMapperClass(CrawlAnalysisMapper.class);
        job1.setCombinerClass(CrawlAnalysisReducer.class);   // optimisation réseau
        job1.setReducerClass(CrawlAnalysisReducer.class);

        // Types clé/valeur
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        // Formats d'entrée/sortie
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Nombre de reducers (à ajuster selon le cluster)
        job1.setNumReduceTasks(4);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(job1Output));

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.err.println("ERREUR : Job 1 a échoué !");
            return 1;
        }
        System.out.println("Job 1 terminé avec succès -> " + job1Output);

        // =====================================================================
        // JOB 2 : Total de pages par entreprise
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println(" JOB 2 : Total pages par entreprise");
        System.out.println("========================================");

        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "CommonCrawl - Totaux par candidat");
        job2.setJarByClass(CrawlAnalysisDriver.class);

        job2.setMapperClass(CompanyTotalMapper.class);
        job2.setCombinerClass(LongSumReducer.class);
        job2.setReducerClass(LongSumReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setNumReduceTasks(1);   // 1 reducer -> fichier trié unique

        FileInputFormat.setInputPaths(job2, new Path(job1Output));
        FileOutputFormat.setOutputPath(job2, new Path(job2Output));

        boolean job2Success = job2.waitForCompletion(true);
        if (!job2Success) {
            System.err.println("ERREUR : Job 2 a échoué !");
            return 1;
        }
        System.out.println("Job 2 terminé avec succès -> " + job2Output);

        // =====================================================================
        // JOB 3 : Répartition par langue
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println(" JOB 3 : Répartition par langue");
        System.out.println("========================================");

        Configuration conf3 = getConf();
        Job job3 = Job.getInstance(conf3, "CommonCrawl - Distribution des langues");
        job3.setJarByClass(CrawlAnalysisDriver.class);

        job3.setMapperClass(LanguageDistributionMapper.class);
        job3.setCombinerClass(LongSumReducer.class);
        job3.setReducerClass(LongSumReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job3, new Path(job1Output));
        FileOutputFormat.setOutputPath(job3, new Path(job3Output));

        boolean job3Success = job3.waitForCompletion(true);
        if (!job3Success) {
            System.err.println("ERREUR : Job 3 a échoué !");
            return 1;
        }
        System.out.println("Job 3 terminé avec succès -> " + job3Output);

        // =====================================================================
        // Résumé
        // =====================================================================
        System.out.println("\n========================================");
        System.out.println(" TOUS LES JOBS TERMINÉS");
        System.out.println("========================================");
        System.out.println("Résultats disponibles dans : " + outputBase);
        System.out.println("  Détail (entreprise/pays/langue) : " + job1Output);
        System.out.println("  Totaux par entreprise           : " + job2Output);
        System.out.println("  Répartition par langue          : " + job3Output);
        System.out.println();
        System.out.println("Pour afficher les résultats :");
        System.out.println("  hdfs dfs -cat " + job2Output + "/part-r-00000");
        System.out.println("  hdfs dfs -cat " + job3Output + "/part-r-00000");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CrawlAnalysisDriver(), args);
        System.exit(exitCode);
    }
}
