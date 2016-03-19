// Snellere, concurrent OpenAC backup.
// Dezelfde backup, maar sequentieel i.p.v. concurrent, is 50% trager.
// 2:26 om 16:24 met backup op sd-kaart zonder escaping
// 3:35 met escaping. Escaping heeft ruim een minuut aan de tijd toegevoegd,
// dat moet sneller kunnen.
// idee: join eerst alle velden met ||, escape het resultaat en vervang
// als laatste || weer door \t. Dat scheelt veel functie calls.
// Na implementatie van bovenstaand idee is de tijd weer terug naar 2:28!
// Volgende optimalisatie: buffered IO.
// 2:09 met buffered IO.
//
//
// Idee voor restore:
// Voor elk tsv-bestand:
// * Lees kolomnamen uit tsv-bestand
// * Lees met een query de structuur van deze tabel uit de database
// * Maak een map[tabelnaam:kolomnaam]datatype t.b.v. dataconversie
// * Construeer een query op basis van tsv-kolommen die voorkomen in de tabel
// * Gebruik zoveel mogelijk batch inserts, commit na elke batch
package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var (
	reTabel = regexp.MustCompile(`(tabel\()([a-z-]+)(\))`)
)

const (
	TSV_BUFFER_SIZE = 300
	SQL_BUFFER_SIZE = 150
)

func DisableConstraints(db *sql.DB, dbms string) {
	var q string
	if dbms == "mssql" {
		q = `EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"`
	} else {
		q = `SET FOREIGN_KEY_CHECKS=0`
	}
	db.Exec(q)
}

func EnableConstraints(db *sql.DB, dbms string) {
	var q string
	if dbms == "mssql" {
		q = `EXEC sp_msforeachtable "ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all"`
	} else {
		q = `SET FOREIGN_KEY_CHECKS=1`
	}
	db.Exec(q)
}

func ColumnTypesMssql(db *sql.DB, table string, columns []string) (map[string]string, error) {
	lookup := make(map[string]bool)
	c := make(map[string]string)
	for _, column := range columns {
		lookup[column] = true
	}
	q := `SELECT COLUMN_NAME, DATA_TYPE
 		FROM information_schema.columns 
 		WHERE table_name = ?`
	rows, err := db.Query(q, table)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var cName, cType string
		if err := rows.Scan(&cName, &cType); err != nil {
			return c, err
		}
		if lookup[cName] {
			c[cName] = cType
		}
	}
	return c, nil
}

func ColumnTypesMysql(db *sql.DB, table string, columns []string) (map[string]string, error) {
	lookup := make(map[string]bool)
	c := make(map[string]string)
	for _, column := range columns {
		lookup[column] = true
	}
	q := fmt.Sprintf("DESCRIBE %s", table)
	rows, err := db.Query(q)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var cName, cType string
		var x1, x2, x3, x4 interface{}
		if err := rows.Scan(&cName, &cType, &x1, &x2, &x3, &x4); err != nil {
			return c, err
		}
		if lookup[cName] {
			c[cName] = cType
		}
	}
	return c, nil
}

func ValueTemplate(db *sql.DB, table string, columns []string) (string, error) {
	values := make([]string, 0)
	// TODO test op dbms "mysql" of "mssql"
	cTypes, err := ColumnTypesMysql(db, table, columns)
	if err != nil {
		return "", err
	}
	for _, column := range columns {
		if cType, ok := cTypes[column]; ok {
			if strings.HasPrefix(cType, "int") {
				values = append(values, "%s")
			} else {
				values = append(values, "'%s'")
			}
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(values, ", ")), nil
}

func InsertQuery(db *sql.DB, dbms, table string, columns []string) (string, error) {
	var err error
	var cTypes map[string]string

	if dbms == "mysql" {
		cTypes, err = ColumnTypesMysql(db, table, columns)
	} else {
		cTypes, err = ColumnTypesMssql(db, table, columns)
	}
	if err != nil {
		return "", err
	}
	// TODO: verwijder onderstaande onzin code
	fmt.Println(cTypes["test"])
	q := `INSERT INTO %s
	(%s)
	VALUES`
	return q, nil
}

//func ColumnTypes(db *sql.DB, dbms, table string, columns []string) map[string]string {

//}

func Subtabellen(db *sql.DB) (map[string]bool, error) {
	t := make(map[string]bool)
	q := `SELECT structuur_tabel_veld
	FROM structuur_data
	WHERE structuur_type LIKE 'subtabel%'`
	rows, err := db.Query(q)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return t, err
		}
		t[col] = true
	}
	return t, nil
}

func ReguliereTabellen(db *sql.DB) ([]string, error) {
	t := make([]string, 0)
	q := `SELECT structuur_tabel_veld 
	FROM structuur_data
	WHERE structuur_type = 'codetabel'
	OR structuur_type LIKE 'toptabel%'
	OR structuur_type LIKE 'subtabel%'`
	rows, err := db.Query(q)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return t, err
		}
		if col == "behandeldag-verrichtingen" {
			t = append(t, "behandeldag_verrichtingen")
		} else {
			t = append(t, fmt.Sprintf("%s_data", strings.Replace(col, "-", "_", -1)))
		}
	}
	return t, nil
}

func Koppeltabellen(db *sql.DB, sub map[string]bool) ([]string, error) {
	t := make([]string, 0)
	/*
		Onderstaande query genereert teveel koppeltabellen, bijvoorbeeld
		behandeling_fin_trajecten. Als er een tabelkoppeling is die verwijst
		naar een subtabel (behandeling:fin_trajecten), dan is er
		geen koppeltabel, maar wordt de key van de parent opgenomen
		in de subtabel.
		Oplossing: neem de koppeling, bijv. "tabel(fin-traject)" op in
		de query en kijk of fin-traject voorkomt in een map met subtabellen.
	*/
	q := `SELECT structuur_data.structuur_tabel_veld, structuur_data.structuur_type
	FROM structuur_data
	JOIN structuur_opties
	ON structuur_data.structuur_tabel_veld = structuur_opties.structuur_tabel_veld
	WHERE (
		structuur_data.structuur_type LIKE 'code(%' OR
		structuur_data.structuur_type LIKE 'tabel(%'
	)
	AND structuur_opties.tv_optie_code = 'meerwaardig'`
	rows, err := db.Query(q)
	if err != nil {
		return t, err
	}
	defer rows.Close()
	for rows.Next() {
		var col, stype string
		if err := rows.Scan(&col, &stype); err != nil {
			return t, err
		}
		naam := strings.Replace(strings.Replace(col, ":", "_", -1), "-", "_", -1)
		// Bij een verwijzing naar een subtabel is er geen koppeltabel.
		matches := reTabel.FindSubmatch([]byte(stype))
		if matches == nil {
			// ok, geen tabelkoppeling
			t = append(t, naam)
		} else {
			// een tabelkoppeling
			verwijzing := string(matches[2])
			if !sub[verwijzing] {
				// ok, verwijzing is geen subtabel
				t = append(t, naam)
			}
		}
	}
	return t, nil
}

func Tabellen(db *sql.DB) ([]string, error) {
	regulier, err := ReguliereTabellen(db)
	if err != nil {
		return regulier, err
	}
	sub, err := Subtabellen(db)
	if err != nil {
		return regulier, err
	}
	koppel, err := Koppeltabellen(db, sub)
	if err != nil {
		return regulier, err
	}
	koppel = append(koppel, "afspraak_item_index")
	koppel = append(koppel, "patient_index")
	koppel = append(koppel, "relatie_index")
	return append(regulier, koppel...), nil
}

func Escape(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		"\n", "\\n",
		"\t", "\\t",
		"\r", "\\r",
		"'", "\\'",
		"||", "\t")
	return r.Replace(s)
}

func RowToStrings(rows *sql.Rows) ([]string, error) {
	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	pValues := make([]interface{}, len(columns))
	output := make([]string, len(columns))

	for i, _ := range columns {
		pValues[i] = &values[i]
	}

	err := rows.Scan(pValues...)
	if err != nil {
		return output, err
	}

	for i, _ := range columns {
		switch s := values[i].(type) {
		case string:
			output[i] = s
		case []byte:
			output[i] = string(s)
		case int, int16, int32, int64:
			output[i] = fmt.Sprintf("%d", s)
		case nil:
			output[i] = "-"
		}
	}
	return output, nil
}

func BackupTabel(db *sql.DB, tabel, dir string) {
	bestandsnaam := path.Join(dir, fmt.Sprintf("%s.tsv", tabel))
	fp, err := os.Create(bestandsnaam)
	if err != nil {
		fmt.Println("Fout bij het aanmaken van '", bestandsnaam, "' : ", err.Error())
		return
	}
	defer fp.Close()
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tabel))
	if err != nil {
		fmt.Println("Fout bij de backup van tabel ", tabel, " : ", err.Error())
		return
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		fmt.Println("Geen kolommen bij tabel ", tabel, " : ", err.Error())
		return
	}
	w := bufio.NewWriter(fp)
	w.Write([]byte(strings.Join(cols, "\t")))
	w.Write([]byte("\n"))
	i := 0
	for rows.Next() {
		cols, err := RowToStrings(rows)
		if err != nil {
			fmt.Println("Fout bij de backup van tabel ", tabel, " : ", err.Error())
			return
		}
		w.Write([]byte(Escape(strings.Join(cols, "||"))))
		w.Write([]byte("\n"))
		if i == TSV_BUFFER_SIZE {
			i = 0
			w.Flush()
		}
	}
	w.Flush()
}

func RestoreTabel(db *sql.DB, tabel string) {
	fp, err := os.Open(tabel)
	if err != nil {
		fmt.Println("Fout bij het openen van tabel: ", tabel, ": ", err.Error())
		return
	}
	tabelnaam := strings.Replace(filepath.Base(tabel), ".tsv", "", 1)
	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s", tabelnaam))
	if err != nil {
		fmt.Println("Fout bij het wissen van tabel: ", tabelnaam, ": ", err.Error())
		return
	}
	repl := strings.NewReplacer(", -", ", NULL", "'-'", "NULL")
	
	r := csv.NewReader(fp)
	r.Comma = '\t'
	r.Comment = '#'
	r.LazyQuotes = true
	eersteRecord := true
	var q, valueTemplate string
	var velden []interface{}
	b := make([]string, SQL_BUFFER_SIZE)
	batchCount := 0

	for {
		record, err := r.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Fout bij het lezen van regel uit tabel: ", tabel, ": ", err.Error())
			continue
		}

		if eersteRecord {
			velden = make([]interface{}, len(record))
			eersteRecord = false
			// TODO niet domweg de velden uit het tsv-bestand overnemen in de query,
			// maar de velden die in de database zitten
			valueTemplate, err = ValueTemplate(db, tabelnaam, record)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			q = fmt.Sprintf(`INSERT INTO %s
			(%s)
			VALUES
			`, tabelnaam, strings.Join(record, ", "))
		} else {
			// TODO: record toevoegen aan values met quoting gebaseerd op type
			for i, veld := range(record) {
				velden[i] = veld
			}
			b[batchCount] = fmt.Sprintf(valueTemplate, velden...)
			batchCount++
			if batchCount == SQL_BUFFER_SIZE {
				_, err = db.Exec(q + repl.Replace(strings.Join(b, ",\n")))
				if err != nil {
					fmt.Println(err.Error())
				}
				batchCount = 0
			}
		}
	}
	if batchCount != 0 {
		_, err = db.Exec(q + repl.Replace(strings.Join(b[:batchCount], ",\n")))
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func RestoreTabellen(db *sql.DB, dir string) {
//	var wg sync.WaitGroup
	DisableConstraints(db, "mysql")
	defer EnableConstraints(db, "mysql")
	tabellen, _ := filepath.Glob(path.Join(dir, "*.tsv"))

	for _, tabel := range tabellen {
//		wg.Add(1)
		fmt.Println(tabel)
		RestoreTabel(db, tabel)
//		go func(tabel string) {
//			defer wg.Done()
//			DisableConstraints(db, "mysql")
//			RestoreTabel(db, tabel)
//		}(tabel)
	}

//	wg.Wait()
}

func BackupTabellen(db *sql.DB, tabellen []string, dir string) {
	var wg sync.WaitGroup

	for _, tabel := range tabellen {
		wg.Add(1)
		go func(tabel string) {
			defer wg.Done()
			BackupTabel(db, tabel, dir)
		}(tabel)
	}

	wg.Wait()
}

func main() {
	db, err := sql.Open("mysql", "root:borland@/test")
	if err != nil {
		fmt.Println("Kon geen verbinding maken met de database: ", err.Error())
		return
	}
	db.SetMaxOpenConns(5)
	tabellen, err := Tabellen(db)
	if err != nil {
		fmt.Println("Fout bij het aanroepen van functie Tabellen(): ", err.Error())
		return
	}
	if len(tabellen) == 0 {
		fmt.Println("Geen OpenAC tabellen in de database")
		return
	}
	//BackupTabellen(db, tabellen, "d:/documents/openac/backup")
	RestoreTabellen(db, "d:/documents/openac/backup")
}
