package bigdata.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Registre des mots-clés politiques pour l'analyse de l'élection 2024.
 *
 * Remplace l'ancienne logique de matching par domaine.
 * Ici on cherche si l'URL ou le TITRE de la page contient un mot-clé
 * associé à un candidat.
 *
 * Format du fichier CSV :
 *   mot_cle,candidat,categorie
 *   trump,Trump,candidate
 *   kamala,Harris,candidate
 *   biden,Biden,candidate
 *
 * Stratégie de matching :
 *   - Recherche insensible à la casse
 *   - Priorité au TITRE (plus informatif que l'URL)
 *   - Si plusieurs candidats matchent sur la même page → on prend le premier trouvé
 *     (ordre du fichier CSV = ordre de priorité)
 *
 * Chargé une seule fois dans le setup() du Mapper via le Distributed Cache.
 */
public class CompanyRegistry {

    private static class KeywordEntry {
        final String keyword;
        final String candidate;
        KeywordEntry(String k, String c) { this.keyword = k; this.candidate = c; }
    }

    private final List<KeywordEntry> entries = new ArrayList<>();

    public void loadFromLocalFile(String localPath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(localPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split(",", 3);
                if (parts.length < 2) continue;
                String keyword   = parts[0].trim().toLowerCase();
                String candidate = parts[1].trim();
                if (keyword.isEmpty() || candidate.isEmpty()) continue;
                entries.add(new KeywordEntry(keyword, candidate));
            }
        }
        System.out.println("[ElectionMapper] " + entries.size() + " mots-clés chargés.");
    }

    /**
     * Cherche si l'URL ou le titre de la page mentionne un candidat connu.
     *
     * @param url   URL de la page (ex: "https://cnn.com/2024/11/trump-wins")
     * @param title Titre HTML de la page (ex: "Trump wins 2024 election - CNN")
     * @return nom du candidat trouvé (ex: "Trump"), ou null si aucun match
     */
    public String findCompany(String url, String title) {
        String urlLower   = (url   != null) ? url.toLowerCase()   : "";
        String titleLower = (title != null) ? title.toLowerCase() : "";

        for (KeywordEntry e : entries) {
            // Le titre est prioritaire : plus précis que l'URL
            if (!titleLower.isEmpty() && titleLower.contains(e.keyword)) {
                return e.candidate;
            }
            if (!urlLower.isEmpty() && urlLower.contains(e.keyword)) {
                return e.candidate;
            }
        }
        return null;
    }

    /** Compatibilité avec l'ancien code (URL seule) */
    public String findCompany(String url) {
        return findCompany(url, null);
    }

    public int size() { return entries.size(); }
}
