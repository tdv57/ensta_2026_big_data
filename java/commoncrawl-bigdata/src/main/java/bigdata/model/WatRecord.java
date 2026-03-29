package bigdata.model;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Représente un enregistrement WAT de Common Crawl parsé depuis une ligne JSON.
 *
 * CORRECTION v2 :
 * ---------------
 * - Ajout de l'extraction du TITRE de la page (Head.Title) : indispensable pour
 *   détecter des pages sur Trump/Harris/Biden dont l'URL ne contient pas le nom.
 *   Ex: "cnn.com/politics/2024/11/05/results" → titre "Trump wins 2024 election"
 *
 * - Correction du chemin JSON vers la langue détectée :
 *   Chemin correct : Envelope > Payload-Metadata > HTTP-Response-Metadata
 *                    > HTML-Metadata > Languages[0].code
 *
 * - Optimisation : on ignore immédiatement les lignes ne commençant pas par '{'
 *   (en-têtes WARC textuels : "WARC/1.0", "WARC-Type: ...", etc.)
 *   Ce qui réduit drastiquement les PARSE_ERRORS dans les compteurs Hadoop.
 *
 * Structure JSON WAT (simplifiée) :
 * {
 *   "Envelope": {
 *     "WARC-Header-Metadata": {
 *       "WARC-Type": "response",
 *       "WARC-Target-URI": "https://cnn.com/2024/11/trump-wins",
 *       "WARC-IP-Address": "151.101.1.67"
 *     },
 *     "Payload-Metadata": {
 *       "HTTP-Response-Metadata": {
 *         "Headers": { "Content-Language": "en" },
 *         "HTML-Metadata": {
 *           "Head": { "Title": "Trump wins 2024 presidential election - CNN" },
 *           "Languages": [
 *             {"count": 820, "code": "en"},
 *             {"count": 12,  "code": "fr"}
 *           ]
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class WatRecord {

    private final String targetUri;
    private final String pageTitle;
    private final String contentLanguage;
    private final String detectedLanguage;

    private WatRecord(Builder b) {
        this.targetUri        = b.targetUri;
        this.pageTitle        = b.pageTitle;
        this.contentLanguage  = b.contentLanguage;
        this.detectedLanguage = b.detectedLanguage;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Tente de parser une ligne en WatRecord.
     * Retourne null si :
     * - La ligne ne commence pas par '{' (en-têtes WARC textuels → ignorés rapidement)
     * - Le JSON est invalide
     * - L'enregistrement n'est pas de type "response"
     */
    public static WatRecord fromJson(String jsonLine) {
        if (jsonLine == null || jsonLine.isEmpty()) return null;
        // Filtre rapide : les en-têtes WARC ("WARC/1.0", "Content-Type: ...", etc.)
        // ne commencent jamais par '{'. Cela évite de tenter un parse JSON inutile
        // et réduit massivement les PARSE_ERRORS dans les compteurs Hadoop.
        if (jsonLine.charAt(0) != '{') return null;

        try {
            JsonObject root     = JsonParser.parseString(jsonLine).getAsJsonObject();
            JsonObject envelope = root.getAsJsonObject("Envelope");
            if (envelope == null) return null;

            // --- En-têtes WARC ---
            JsonObject warcHeaders = envelope.getAsJsonObject("WARC-Header-Metadata");
            if (warcHeaders == null) return null;

            // On ne traite que les enregistrements "response" (pages HTML)
            String warcType = getStr(warcHeaders, "WARC-Type");
            if (!"response".equalsIgnoreCase(warcType)) return null;

            String uri = getStr(warcHeaders, "WARC-Target-URI");

            // --- Payload ---
            String title        = null;
            String contentLang  = null;
            String detectedLang = null;

            JsonObject payload = envelope.getAsJsonObject("Payload-Metadata");
            if (payload != null) {
                JsonObject httpMeta = payload.getAsJsonObject("HTTP-Response-Metadata");
                if (httpMeta != null) {

                    // Langue depuis l'en-tête HTTP Content-Language
                    JsonObject headers = httpMeta.getAsJsonObject("Headers");
                    if (headers != null) {
                        contentLang = normalizeLanguage(getStr(headers, "Content-Language"));
                    }

                    JsonObject htmlMeta = httpMeta.getAsJsonObject("HTML-Metadata");
                    if (htmlMeta != null) {

                        // Titre de la page — crucial pour matcher les candidats
                        // même quand l'URL ne contient pas leur nom
                        JsonObject head = htmlMeta.getAsJsonObject("Head");
                        if (head != null) {
                            title = getStr(head, "Title");
                        }

                        // Langue détectée dans le contenu HTML
                        // Chemin corrigé : HTML-Metadata > Languages (et non plus
                        // HTTP-Response-Metadata > HTML-Metadata > Languages comme avant)
                        JsonElement langs = htmlMeta.get("Languages");
                        if (langs != null && langs.isJsonArray()) {
                            JsonArray arr = langs.getAsJsonArray();
                            if (arr.size() > 0) {
                                JsonObject first = arr.get(0).getAsJsonObject();
                                detectedLang = normalizeLanguage(getStr(first, "code"));
                            }
                        }
                    }
                }
            }

            return new Builder()
                    .targetUri(uri)
                    .pageTitle(title)
                    .contentLanguage(contentLang)
                    .detectedLanguage(detectedLang)
                    .build();

        } catch (Exception e) {
            // JSON malformé ou structure inattendue : on ignore
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Accesseurs
    // -------------------------------------------------------------------------

    public String getTargetUri()  { return targetUri  != null ? targetUri  : ""; }
    public String getPageTitle()  { return pageTitle  != null ? pageTitle  : ""; }

    /**
     * Langue la plus fiable disponible :
     * 1. Langue détectée dans le HTML (priorité maximale)
     * 2. En-tête HTTP Content-Language
     * 3. "unknown"
     */
    public String getBestLanguage() {
        if (detectedLanguage != null && !detectedLanguage.isEmpty()) return detectedLanguage;
        if (contentLanguage  != null && !contentLanguage.isEmpty())  return contentLanguage;
        return "unknown";
    }

    /**
     * Approximation du pays cible via le TLD de l'URL.
     * TLDs génériques (.com .net .org .gov .edu .io) → "com" (pays indéterminé).
     * Exemples : bbc.co.uk → "uk", lefigaro.fr → "fr", dw.de → "de"
     */
    public String getTldCountry() {
        try {
            String host = getTargetUri();
            if (host.contains("://")) host = host.substring(host.indexOf("://") + 3);
            if (host.contains("/"))   host = host.substring(0, host.indexOf('/'));
            if (host.contains("?"))   host = host.substring(0, host.indexOf('?'));
            if (host.contains(":"))   host = host.substring(0, host.lastIndexOf(':'));
            // Cas spécial co.uk, co.jp etc → prendre l'avant-dernier segment
            String[] parts = host.split("\\.");
            if (parts.length < 2) return "unknown";
            String tld = parts[parts.length - 1].toLowerCase();
            if (tld.equals("com") || tld.equals("net") || tld.equals("org")
                    || tld.equals("info") || tld.equals("io")
                    || tld.equals("gov")  || tld.equals("edu")) {
                return "com";
            }
            return tld;
        } catch (Exception e) {
            return "unknown";
        }
    }

    // -------------------------------------------------------------------------
    // Utilitaires privés
    // -------------------------------------------------------------------------

    private static String getStr(JsonObject obj, String key) {
        JsonElement el = obj.get(key);
        return (el != null && !el.isJsonNull()) ? el.getAsString() : null;
    }

    /** Normalise : "fr-FR", "FR", "fr_FR" → "fr" */
    private static String normalizeLanguage(String lang) {
        if (lang == null || lang.isEmpty()) return null;
        lang = lang.toLowerCase().trim();
        if (lang.contains("-")) lang = lang.substring(0, lang.indexOf('-'));
        if (lang.contains("_")) lang = lang.substring(0, lang.indexOf('_'));
        return lang.length() <= 10 ? lang : null;
    }

    private static class Builder {
        String targetUri, pageTitle, contentLanguage, detectedLanguage;
        Builder targetUri(String v)        { this.targetUri = v; return this; }
        Builder pageTitle(String v)        { this.pageTitle = v; return this; }
        Builder contentLanguage(String v)  { this.contentLanguage = v; return this; }
        Builder detectedLanguage(String v) { this.detectedLanguage = v; return this; }
        WatRecord build()                  { return new WatRecord(this); }
    }
}
