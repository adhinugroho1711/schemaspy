/*
 * Copyright (C) 2004 - 2011, 2014 John Currier
 * Copyright (C) 2016 Rafal Kasa
 * Copyright (C) 2016, 2017 Ismail Simsek
 * Copyright (C) 2017 Mårten Bohlin
 * Copyright (C) 2018 Nils Petzaell
 *
 * This file is part of SchemaSpy.
 *
 * SchemaSpy is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SchemaSpy is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with SchemaSpy. If not, see <http://www.gnu.org/licenses/>.
 */
package org.schemaspy.analyzer;

import org.schemaspy.model.DatabaseObject;
import org.schemaspy.model.ImpliedForeignKeyConstraint;
import org.schemaspy.model.Table;
import org.schemaspy.model.TableColumn;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kelas ini mencari kendala kunci asing yang tersirat (implied foreign key constraints) dalam skema database.
 * Ia menganalisis kolom dalam tabel dan menemukan kolom yang mungkin berfungsi sebagai kunci asing
 * tetapi tidak dideklarasikan sebagai kunci asing di skema database.
 */
public class ImpliedConstraintsFinder {

    /**
     * Konstanta untuk menyimpan nama kolom yang harus dikecualikan dari pencarian kendala tersirat.
     * TODO: Sebaiknya dipindahkan ke file konfigurasi atau properti SchemaSpy.
     */
    private static final String EXCLUDED_COLUMN = "LanguageId";

    /**
     * Mencari kendala kunci asing tersirat dalam koleksi tabel yang diberikan.
     *
     * @param tables Koleksi tabel yang akan dianalisis
     * @return Daftar kendala kunci asing tersirat yang ditemukan
     */
    public List<ImpliedForeignKeyConstraint> find(Collection<Table> tables) {
        // Cari kolom-kolom yang tidak memiliki parent
        List<TableColumn> columnsWithoutParents =
            tables
                .stream()
                .map(Table::getColumns)
                .flatMap(Collection::stream)
                .filter(this::noParent)
                .sorted(byTable)
                .collect(Collectors.toList());

        // Dapatkan pemetaan untuk primary key
        Map<DatabaseObject, Table> keyedTablesByPrimary = primaryKeys(tables);

        List<ImpliedForeignKeyConstraint> impliedConstraints = new ArrayList<>();

        for (TableColumn childColumn : columnsWithoutParents) {
            DatabaseObject columnWithoutParent = new DatabaseObject(childColumn);

            // Cari tabel Parent (PK)
            Table primaryTable = findPrimaryTable(columnWithoutParent, keyedTablesByPrimary);

            if (primaryTable != null && primaryTable != childColumn.getTable()) {
                // Tidak dapat mencocokkan multiple primary key untuk saat ini
                // sehingga hanya memeriksa kolom PK pertama
                List<TableColumn> primaryColumns = primaryTable.getPrimaryColumns();
                if (primaryColumns != null && !primaryColumns.isEmpty()) {
                    TableColumn parentColumn = primaryColumns.get(0);
                    // Pastikan hubungan potensial child->parent belum menjadi hubungan parent->child
                    if (parentColumn.getParentConstraint(childColumn) == null) {
                        // Kita telah menemukan hubungan potensial dengan kolom yang cocok dengan kolom primary key
                        // di tabel lain dan belum terkait dengan kolom tersebut
                        impliedConstraints.add(new ImpliedForeignKeyConstraint(parentColumn, childColumn));
                    }
                }
            }
        }
        return impliedConstraints;
    }

    /**
     * Memeriksa apakah kolom tidak memiliki parent dan dapat digunakan dalam pencarian kendala tersirat.
     *
     * @param column Kolom yang akan diperiksa
     * @return true jika kolom tidak memiliki parent dan dapat digunakan dalam pencarian kendala tersirat
     */
    private boolean noParent(TableColumn column) {
        return !column.isForeignKey()
               && !column.isPrimary()
               && column.allowsImpliedParents()
               && !EXCLUDED_COLUMN.equals(column.getName());
    }

    /**
     * Komparator untuk mengurutkan kolom berdasarkan tabel dan nama kolom.
     */
    private Comparator<TableColumn> byTable = (column1, column2) -> {
        int rc = column1.getTable().compareTo(column2.getTable());
        if (rc == 0) {
            rc = column1.getName().compareToIgnoreCase(column2.getName());
        }
        return rc;
    };

    /**
     * Mengumpulkan semua primary key dari koleksi tabel dan mengembalikannya dalam bentuk peta.
     *
     * @param tables Koleksi tabel yang akan diproses
     * @return Peta yang memetakan objek database primary key ke tabel yang memilikinya
     */
    private Map<DatabaseObject, Table> primaryKeys(Collection<Table> tables) {
        Map<DatabaseObject, Table> keyedTablesByPrimary = new TreeMap<>();

        for (Table table : tables) {
            List<TableColumn> tablePrimaries = table.getPrimaryColumns();
            // Lanjutkan hanya jika ada primary columns dan memenuhi kondisi
            if (tablePrimaries != null && !tablePrimaries.isEmpty() &&
                (tablePrimaries.size() == 1 || tablePrimaries.stream().anyMatch(t -> EXCLUDED_COLUMN.equals(t.getName())))) {
                // Tidak dapat mencocokkan multiple primary key untuk saat ini
                TableColumn tableColumn = tablePrimaries.get(0);
                if (tableColumn != null) {
                    DatabaseObject primary = new DatabaseObject(tableColumn);
                    if (tableColumn.allowsImpliedChildren()) {
                        // Primary key baru (nama/tipe)
                        keyedTablesByPrimary.put(primary, table);
                    }
                }
            }
        }
        return keyedTablesByPrimary;
    }

    /**
     * Mencari tabel primary yang cocok dengan kolom tanpa parent.
     *
     * @param columnWithoutParent Objek database yang mewakili kolom tanpa parent
     * @param keyedTablesByPrimary Peta yang memetakan objek database primary key ke tabel yang memilikinya
     * @return Tabel primary yang cocok, atau null jika tidak ditemukan atau ada ambiguitas
     */
    private Table findPrimaryTable(DatabaseObject columnWithoutParent, Map<DatabaseObject, Table> keyedTablesByPrimary) {
        Table primaryTable = null;
        for (Map.Entry<DatabaseObject, Table> entry : keyedTablesByPrimary.entrySet()) {
            if (isPotentialPrimaryTableMatch(columnWithoutParent, entry)) {
                // Jika kolom child mereferensikan beberapa tabel PK(Parent), maka jangan buat implied relationship
                // dan keluar dari loop. Satu kolom hanya dapat mereferensikan satu tabel parent!
                if (Objects.nonNull(primaryTable)) {
                    return null;
                }
                primaryTable = entry.getValue();
            }
        }
        return primaryTable;
    }
    
    /**
     * Memeriksa apakah entry peta adalah tabel primary potensial untuk kolom tanpa parent.
     *
     * @param columnWithoutParent Objek database yang mewakili kolom tanpa parent
     * @param entry Entry peta yang berisi objek database primary key dan tabel yang memilikinya
     * @return true jika entry peta adalah tabel primary potensial untuk kolom tanpa parent
     */
    private boolean isPotentialPrimaryTableMatch(DatabaseObject columnWithoutParent, Map.Entry<DatabaseObject, Table> entry) {
        DatabaseObject key = entry.getKey();
        return nameMatches(columnWithoutParent.getName(), key.getName(), entry.getValue().getName())
               && typeMatches(columnWithoutParent, key);
    }

    /**
     * Memeriksa apakah nama kolom tanpa parent cocok dengan nama primary key atau tabel primary key.
     *
     * @param columnWithoutParent Nama kolom tanpa parent
     * @param primaryKey Nama primary key
     * @param primaryKeyTable Nama tabel primary key
     * @return true jika nama kolom tanpa parent cocok dengan nama primary key atau tabel primary key
     */
    private boolean nameMatches(String columnWithoutParent, String primaryKey, String primaryKeyTable) {
        if (columnWithoutParent == null || primaryKey == null || primaryKeyTable == null) {
            return false;
        }
        return columnWithoutParent.compareToIgnoreCase(primaryKey) == 0
               || columnWithoutParent.matches("(?i).*_" + Pattern.quote(primaryKey))
               || columnWithoutParent.matches("(?i)" + Pattern.quote(primaryKeyTable) + ".*" + Pattern.quote(primaryKey));
    }

    /**
     * Memeriksa apakah tipe kolom tanpa parent cocok dengan tipe primary key.
     *
     * @param orphan Objek database yang mewakili kolom tanpa parent
     * @param primaryKey Objek database yang mewakili primary key
     * @return true jika tipe kolom tanpa parent cocok dengan tipe primary key
     */
    private boolean typeMatches(DatabaseObject orphan, DatabaseObject primaryKey) {
        if (orphan == null || primaryKey == null) {
            return false;
        }
        // Periksa kecocokan tipe dan panjang
        boolean typeMatchByObject = orphan.getType() != null && primaryKey.getType() != null
                                    && orphan.getType().compareTo(primaryKey.getType()) == 0;
        boolean typeMatchByName = orphan.getTypeName() != null && primaryKey.getTypeName() != null
                                  && orphan.getTypeName().compareToIgnoreCase(primaryKey.getTypeName()) == 0;
        boolean lengthMatch = orphan.getLength() - primaryKey.getLength() == 0;
        
        return (typeMatchByObject || typeMatchByName) && lengthMatch;
    }
}
