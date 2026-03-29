package bigdata.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper du Job 2 : Agrégation par entreprise uniquement
 *
 * Lit la sortie du Job 1 (TSV : entreprise\tpays\tlangue\tcount)
 * et réémet (entreprise, count) pour obtenir le total de pages
 * par entreprise, toutes langues et pays confondus.
 *
 * Utilisé pour produire le classement global :
 *   Toyota         : 142 000 pages
 *   Volkswagen     : 98 500 pages
 *   ...
 */
public class CompanyTotalMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text company = new Text();
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
            company.set(parts[0]);
            count.set(Long.parseLong(parts[3]));
            context.write(company, count);
        } catch (NumberFormatException e) {
            // Ignorer les lignes malformées
        }
    }
}
