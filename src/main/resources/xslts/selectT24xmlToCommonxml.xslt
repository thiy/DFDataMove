<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="2.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:template match="/Select">
		<database>
			<xsl:for-each select="*">
				<xsl:element name="table">
					<xsl:attribute name="tableName">
		                 <xsl:value-of select="name()" />
			        </xsl:attribute>
					<xsl:attribute name="action">
		                 <xsl:text>insert</xsl:text>
			        </xsl:attribute>
					<xsl:for-each select="*">
						<xsl:element name="column">
							<xsl:attribute name="columnName">
				                 <xsl:value-of select="name()" />
					        </xsl:attribute>
        					<xsl:if test="@isPrimaryKey = 'true'">
								<xsl:attribute name="isPrimaryKey">
					                 <xsl:text>true</xsl:text>
						        </xsl:attribute>
							</xsl:if>			        
							<xsl:call-template name="replace-single-quote"><xsl:with-param name="text" select="." /></xsl:call-template>
						</xsl:element>
					</xsl:for-each>
				</xsl:element>

			</xsl:for-each>
		</database>
	</xsl:template>
	
	
  <!-- recursive template to escape single quotes -->
  <xsl:template name="replace-single-quote">
    <xsl:param name="text"/>
    <xsl:param name="replace" select="&quot;'&quot;"/>
    <xsl:param name="with" select="&quot;&quot;"/>
    <xsl:choose>
      <xsl:when test="contains($text,$replace)">
        <xsl:value-of select="substring-before($text,$replace)"/>
        <xsl:value-of select="$with"/>
        <xsl:call-template name="replace-single-quote">
          <xsl:with-param name="text"
                          select="substring-after($text,$replace)"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$text"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
	
	
</xsl:stylesheet>