# İhale MCP: Türkiye Kamu İhaleleri için MCP Sunucusu

Bu proje, Türkiye'deki kamu ihalelerine (`ekap.kik.gov.tr`) erişimi kolaylaştıran bir [FastMCP](https://gofastmcp.com/) sunucusu oluşturur. Bu sayede, EKAP v2 portalından ihale arama, ihale detaylarını getirme ve ihale duyurularını Markdown formatında alma işlemleri, Model Context Protocol (MCP) destekleyen LLM (Büyük Dil Modeli) uygulamaları (örneğin Claude Desktop veya [5ire](https://5ire.app)) ve diğer istemciler tarafından araç (tool) olarak kullanılabilir hale gelir.

🎯 **Temel Özellikler**

* EKAP v2 portalına programatik erişim için standart bir MCP arayüzü.
* Aşağıdaki yetenekler:
    * **Detaylı İhale Arama:** İhale adı/içeriği, IKN numarası, ihale türü, il, tarih aralıkları ve 17+ boolean filtre ile kapsamlı arama.
    * **İhale Detayları:** Belirli bir ihalenin tam detaylarını (özellikler, OKAS kodları, idare bilgileri, işlem kuralları) getirme.
    * **İhale Duyuruları:** İhale ile ilgili tüm duyuruları (Ön İlan, İhale İlanı, Sonuç İlanı vb.) otomatik HTML-to-Markdown dönüşümü ile getirme.
    * **OKAS Kod Arama:** Türk kamu alım sınıflandırma kodlarında arama yapma.
    * **İdare Arama:** Bakanlık, belediye, üniversite gibi kamu kurumlarını arama.
* İhale metinlerinin LLM'ler tarafından daha kolay işlenebilmesi için HTML'den Markdown formatına çevrilmesi.
* Claude Desktop uygulaması ile kolay entegrasyon.
* İhale MCP, [5ire](https://5ire.app) gibi Claude Desktop haricindeki MCP istemcilerini de destekler.

> Yeni: Google Maps/Places için bağımsız MCP sunucusu eklendi (`maps_mcp.py`).

---
🚀 **Claude Haricindeki Modellerle Kullanmak İçin Çok Kolay Kurulum (Örnek: 5ire için)**

Bu bölüm, İhale MCP aracını 5ire gibi Claude Desktop dışındaki MCP istemcileriyle kullanmak isteyenler içindir.

* **Python Kurulumu:** Sisteminizde Python 3.11 veya üzeri kurulu olmalıdır. Kurulum sırasında "**Add Python to PATH**" (Python'ı PATH'e ekle) seçeneğini işaretlemeyi unutmayın. [Buradan](https://www.python.org/downloads/) indirebilirsiniz.
* **Git Kurulumu (Windows):** Bilgisayarınıza [git](https://git-scm.com/downloads/win) yazılımını indirip kurun. "Git for Windows/x64 Setup" seçeneğini indirmelisiniz.
* **`uv` Kurulumu:**
    * **Windows Kullanıcıları (PowerShell):** Bir CMD ekranı açın ve bu kodu çalıştırın: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
    * **Mac/Linux Kullanıcıları (Terminal):** Bir Terminal ekranı açın ve bu kodu çalıştırın: `curl -LsSf https://astral.sh/uv/install.sh | sh`
* **Microsoft Visual C++ Redistributable (Windows):** Bazı Python paketlerinin doğru çalışması için gereklidir. [Buradan](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170) indirip kurun.
* İşletim sisteminize uygun [5ire](https://5ire.app) MCP istemcisini indirip kurun.
* 5ire'ı açın. **Workspace -> Providers** menüsünden kullanmak istediğiniz LLM servisinin API anahtarını girin.
* **Tools** menüsüne girin. **+Local** veya **New** yazan butona basın.
    * **Tool Key:** `ihalemcp`
    * **Name:** `İhale MCP`
    * **Command:**
        ```
        uvx --from git+https://github.com/saidsurucu/ihale-mcp ihale-mcp
        ```
    * **Save** butonuna basarak kaydedin.
* Şimdi **Tools** altında **İhale MCP**'yi görüyor olmalısınız. Üstüne geldiğinizde sağda çıkan butona tıklayıp etkinleştirin (yeşil ışık yanmalı).
* Artık İhale MCP ile konuşabilirsiniz.

---
⚙️ **Claude Desktop Manuel Kurulumu**

1.  **Ön Gereksinimler:** Python, `uv`, (Windows için) Microsoft Visual C++ Redistributable'ın sisteminizde kurulu olduğundan emin olun. Detaylı bilgi için yukarıdaki "5ire için Kurulum" bölümündeki ilgili adımlara bakabilirsiniz.
2.  Claude Desktop **Settings -> Developer -> Edit Config**.
3.  Açılan `claude_desktop_config.json` dosyasına `mcpServers` altına ekleyin:

    ```json
    {
      "mcpServers": {
        "İhale MCP": {
          "command": "uvx",
          "args": [
           "--from", "git+https://github.com/saidsurucu/ihale-mcp",
           "ihale-mcp"
          ]
        }
      }
    }
    ```
4.  Claude Desktop'ı kapatıp yeniden başlatın.

🛠️ **Kullanılabilir Araçlar (MCP Tools)**

Bu FastMCP sunucusu LLM modelleri için aşağıdaki araçları sunar:

* **`search_tenders`**: EKAP v2 portalında kapsamlı ihale arama yapar.
    * **Ana Parametreler**: `search_text`, `ikn_year`, `ikn_number`, `tender_types`, `tender_date_start/end`, `announcement_date_start/end`
    * **Boolean Filtreler**: `e_ihale`, `ortak_alim_mi`, `kismi_teklif_mi`, `yabanci_isteklilere_izin_veriliyor_mu` ve 13+ daha fazla filtre
    * **Liste Filtreleri**: `provinces`, `tender_statuses`, `tender_methods`, `okas_codes`, `authority_ids`, `proposal_types`, `announcement_types`
    * **Arama Kapsamı**: `search_in_title`, `search_in_announcement`, `search_in_tech_spec` vb. 11 farklı alan
    * **Döndürdüğü Değer**: Sayfalanmış ihale listesi, toplam sonuç sayısı

* **`search_okas_codes`**: OKAS (kamu alım sınıflandırma) kodlarında arama yapar.
    * **Parametreler**: `search_term`, `kalem_turu` (1=Mal, 2=Hizmet, 3=Yapım), `limit`
    * **Döndürdüğü Değer**: OKAS kodları, açıklamaları ve kategorileri

* **`search_authorities`**: Türk kamu kurumlarında arama yapar.
    * **Parametreler**: `search_term`, `limit`
    * **Döndürdüğü Değer**: Kurum ID'leri, isimleri ve hiyerarşik bilgileri

* **`get_recent_tenders`**: Son N gündeki ihaleleri getirir.
* **`find_business_leads`**: Google Maps/Places Text Search kullanarak anahtar kelime + konum ile işletmeleri bulur ve satış lead listesine dönüştürür.
    * **Parametreler**: `keyword`, `location_text`, `radius_meters`, `limit`, `include_details`, `min_rating`, `min_user_ratings_total`, `types_include`, `types_exclude`, `require_phone_or_website`, `only_open_now`, `business_status_in`, `output_format` (json|csv), `csv_columns`, `dedupe_by` (place_id|name_address)
    * **JSON Çıktı**: `leads[]` (name, formatted_address, lat, lng, place_id, types, rating, user_ratings_total, business_status, phone, phone_intl, website, open_now), `total`, `query`, `location`, `filters`
    * **CSV Çıktı**: `csv` alanı (string), `columns`, `total`, `content_type`

### Yapılandırma

Çalıştırmadan önce bir Google Maps API anahtarı tanımlayın:

```bash
export GOOGLE_MAPS_API_KEY="<YOUR_KEY>"
```
---

## Google Maps MCP Sunucusu (Maps)

Bağımsız bir konteyner olarak çalışır. Örnek Docker komutu:

```bash
sudo docker run -d \
  --name maps-sunucu \
  -p 7862:7860 \
  -e GOOGLE_MAPS_API_KEY="<YOUR_KEY>" \
  -i $(sudo docker build -q -f Dockerfile.maps .)
```

Kullanılabilir araçlar:

- `find_business_leads`: Anahtar kelime + konum ile işletmeleri bulur.
  - Parametreler: `keyword`, `location_text`, `radius_meters`, `limit`, `include_details`, `min_rating`, `min_user_ratings_total`, `types_include`, `types_exclude`, `require_phone_or_website`, `only_open_now`, `business_status_in`, `output_format` (json|csv), `csv_columns`, `dedupe_by` (place_id|name_address)
  - Çıktı: JSON veya CSV (string + columns + content_type)

### Google Sheets’e Otomatik Aktarma (Opsiyonel)

`find_business_leads` çıktısını arama yapar yapmaz Google Sheets’e satır satır yazdırabilirsiniz.

#### 1) Google Cloud tarafında yapmanız gerekenler

- **Google Sheets API’yi etkinleştirin**: Google Cloud Console → APIs & Services → Library → “Google Sheets API” → Enable.
- **Servis hesabı oluşturun**: IAM & Admin → Service Accounts → Create.
- **JSON anahtar indirin**: Servis hesabı → Keys → Add key → Create new key → JSON.
- **Sheet’i servis hesabıyla paylaşın**: Hedef Google Sheet’i açın → Share → servis hesabı e-postasını (örn. `xxx@yyy.iam.gserviceaccount.com`) **Editor** olarak ekleyin.

#### 2) Sunucuda (env) ayarlayacağınız değişkenler

Servis hesabını iki şekilde verebilirsiniz:

- **Dosya ile (önerilen)**:

```bash
export GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE="/path/to/service-account.json"
```

- **JSON içeriği ile** (tek satır JSON string):

```bash
export GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'
```

Hedef sheet bilgileri:

```bash
export GOOGLE_SHEETS_SPREADSHEET_ID="<SPREADSHEET_ID>"
export GOOGLE_SHEETS_SHEET_NAME="Leads"   # opsiyonel (default: Leads)
```

> `SPREADSHEET_ID`, sheet URL’sindeki `/d/<ID>/` kısmıdır.

Otomatik yazdırmayı her aramada açmak için (tool parametresi vermeden):

```bash
export GOOGLE_SHEETS_AUTO_EXPORT="true"
```

#### 5 sekmeye (tab) keyword'e göre otomatik yazdırma

Sheet’inizde aşağıdaki tab isimleri varsa (yoksa otomatik oluşturulur):
- `TARIM_MAKINA_BAYI`
- `ILAC_BAYI`
- `ZIRAAT_ODALARI`
- `CIFTCI_KOOPARATIFI`
- `GENEL_TARIM` (fallback)

Keyword’e göre otomatik yönlendirmeyi açın:

```bash
export GOOGLE_SHEETS_AUTO_ROUTE_BY_KEYWORD="true"
```

Eşleşme kurallarını isterseniz env ile özelleştirebilirsiniz (JSON dict: substring -> tab adı):

```bash
export GOOGLE_SHEETS_KEYWORD_TAB_MAP='{
  "tarım makina": "TARIM_MAKINA_BAYI",
  "ilac bayi": "ILAC_BAYI",
  "ziraat odaları": "ZIRAAT_ODALARI",
  "çiftçi kooperatifi": "CIFTCI_KOOPARATIFI",
  "genel tarım": "GENEL_TARIM"
}'
```

#### 3) Tool çağrısında açmanız gereken parametreler

`find_business_leads` çağrısına şunları ekleyin:

- `export_to_google_sheets=true`
- (opsiyonel) `google_sheets_spreadsheet_id="..."`
- (opsiyonel) `google_sheets_sheet_name="Leads"`
- (opsiyonel) `google_sheets_include_raw_json=true` (her satıra raw JSON da yazar)

Başarılı olursa response içinde `google_sheets.ok=true` ve `updated_rows` gibi alanlar döner; hata olursa `google_sheets.ok=false` + `error` döner (arama sonucu yine döner).

#### 4) Docker ile çalıştırırken örnek

Servis hesabı JSON’unu konteynıra “read-only” mount edin:

```bash
sudo docker run -d --name maps-sunucu \
  -p 7862:7860 \
  -e GOOGLE_MAPS_API_KEY="<YOUR_KEY>" \
  -e GOOGLE_SHEETS_SPREADSHEET_ID="<SPREADSHEET_ID>" \
  -e GOOGLE_SHEETS_SHEET_NAME="Leads" \
  -e GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE="/run/secrets/sa.json" \
  -v "/path/to/service-account.json:/run/secrets/sa.json:ro" \
  -i $(sudo docker build -q -f Dockerfile.maps .)
```


> Google Cloud Console'da Places API ve Geocoding API'yi etkinleştirin ve kotaları göz önünde bulundurun.

    * **Parametreler**: `days` (1-30), `tender_types`, `limit`
    * **Döndürdüğü Değer**: Yakın tarihli ihale listesi

* **`get_tender_announcements`**: Belirli bir ihalenin tüm duyurularını getirir.
    * **Parametreler**: `tender_id`, `include_html`
    * **Döndürdüğü Değer**: Otomatik HTML-to-Markdown dönüştürülmüş ihale duyuruları

* **`get_tender_details`**: Belirli bir ihalenin kapsamlı detaylarını getirir.
    * **Parametreler**: `tender_id`
    * **Döndürdüğü Değer**: İhale özellikleri, OKAS kodları, idare bilgileri, işlem kuralları ve otomatik markdown'a çevrilmiş duyuru özetleri

## İhale Türleri

- **1 - Mal**: Malzeme ve ekipman alımları
- **2 - Yapım**: İnşaat ve altyapı projeleri  
- **3 - Hizmet**: Hizmet sözleşmeleri
- **4 - Danışmanlık**: Danışmanlık hizmetleri

## Örnek Kullanımlar

1. **Pazar Araştırması**: Belirli sektör veya bölgelerdeki fırsatları takip etme
2. **Uygunluk İzleme**: Mevzuata uygunluk için ihale duyurularını takip etme
3. **İş Zekası**: Kamu harcama modellerini ve trendlerini analiz etme
4. **Bildirim Sistemleri**: Belirli ihale türleri için uyarı sistemleri kurma
5. **Veri Analizi**: Araştırma ve analiz için ihale verilerini çıkarma

## Yeni Özellikler

✅ **17+ Boolean Filtre**: e-İhale, ortak alım, kısmi teklif, yabancı katılım vb.
✅ **Liste Filtreleri**: İller, ihale durumları, usulleri, OKAS kodları, idare ID'leri
✅ **Arama Kapsamı Kontrolü**: IKN, başlık, duyuru, teknik şartname vb. alanlarda arama
✅ **İdare Arama**: 72,000+ kamu kurumunda arama (bakanlık, belediye, üniversite)
✅ **İhale Duyuruları**: Otomatik HTML-to-Markdown dönüşümü ile tam duyuru metinleri
✅ **Kapsamlı İhale Detayları**: Tüm ihale metadata'sı, özellikler, kurallar bir arada

## API Hız Limitleri

Bu sunucu EKAP portalının hız limitlerini gözetir. Üretim kullanımı için aşağıdakileri göz önünde bulundurun:
- API çağrılarını azaltmak için istek önbellekleme
- Üstel geri çekilme ile yeniden deneme mantığı
- Yüksek hacimli kullanım için istek sırası

📜 **Lisans**

Bu proje MIT Lisansı altında lisanslanmıştır. Detaylar için `LICENSE` dosyasına bakınız.

## Sorumluluk Reddi

Bu, Türk hükümetinin EKAP portalı ile resmi olmayan bir entegrasyondır. Kullanıcılar portalın hizmet şartlarına ve geçerli düzenlemelere uymakla yükümlüdür. Yazarlar Türk hükümeti veya EKAP portalı ile bağlantılı değildir.
